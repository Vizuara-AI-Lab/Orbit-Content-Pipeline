#!/usr/bin/env python3
"""
Vizuara Orbit — Content Pipeline Migration Script

Reads courses from the production Firestore, runs the content pipeline
(transcription → LLM extraction → summary → figures → quizzes → aggregation),
and writes all results to the new Orbit Firestore.

Usage:
    python run_pipeline.py [course_id ...]

    If no course IDs are passed, runs all IDs in COURSE_IDS below.

Prerequisites:
    pip install firebase-admin openai yt-dlp python-dotenv paperbanana

Credentials:
    Place two service account JSON files in this directory:
        prod-service-account.json   ← existing Vizuara Firebase
        orbit-service-account.json  ← new Orbit Firebase

    Create a .env file (or export env vars):
        OPENAI_API_KEY=sk-...
        GOOGLE_API_KEY=...          ← Gemini key, used by PaperBanana for figure generation
        ORBIT_STORAGE_BUCKET=your-orbit-bucket.appspot.com
"""

import os
import re
import sys
import json
import uuid
import time
import tempfile
import subprocess
import traceback
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

# Resolve yt-dlp from the venv so it works without activating the venv manually
_VENV_BIN = Path(sys.executable).parent
YT_DLP = str(_VENV_BIN / "yt-dlp")

import firebase_admin
from firebase_admin import credentials, firestore, storage
from google.cloud.firestore_v1 import SERVER_TIMESTAMP

import openai

# ─── Course IDs to process ────────────────────────────────────────────────────
# Replace with your actual course IDs from the production database.
COURSE_IDS = [
    # "course_id_01",
    # "course_id_02",
    # ... up to 30
]

# ─── Config ───────────────────────────────────────────────────────────────────
PROD_SA   = os.getenv("PROD_SERVICE_ACCOUNT",  "prod-service-account.json")
ORBIT_SA  = os.getenv("ORBIT_SERVICE_ACCOUNT", "orbit-service-account.json")
ORBIT_BUCKET     = os.environ["ORBIT_STORAGE_BUCKET"]   # required
OPENAI_API_KEY   = os.environ["OPENAI_API_KEY"]
GOOGLE_API_KEY   = os.getenv("GOOGLE_API_KEY", "")      # Gemini key for PaperBanana

LLM_MODEL             = "gpt-4o"
MAX_TRANSCRIPT_CHARS  = 90_000   # ~22k tokens
MAX_CHARS_PER_LESSON  = 20_000   # per lesson when building quiz context
FIGURE_PATTERN        = re.compile(r'\[FIGURE:\s*"([^"]+)"\]')

# Style context injected into every PaperBanana figure request.
# Encodes Vizuara's teaching-grade visual preferences.
FIGURE_STYLE_CONTEXT = """
Visual style requirements (non-negotiable):
- Teaching-grade clarity. The diagram must be immediately understandable to a student
  encountering this concept for the first time. Prioritise clarity over completeness.
- Subtle, muted colour palette. No bright or saturated colours. Use soft, pastel-adjacent
  tones. The image should feel calm and professional, not eye-catching or vibrant.
- Minimal text. Labels only where essential. No paragraphs, no bullet lists inside the figure.
- Zero visual clutter. Generous whitespace. Every element must earn its place.
- Borders (if used) must be standard width and rendered in a clearly pronounced, dark colour
  so they are unambiguous against the background.
"""

# ─── Firebase Initialisation ──────────────────────────────────────────────────
_prod_app = firebase_admin.initialize_app(
    credentials.Certificate(PROD_SA),
    name="prod"
)
_orbit_app = firebase_admin.initialize_app(
    credentials.Certificate(ORBIT_SA),
    {"storageBucket": ORBIT_BUCKET},
    name="orbit"
)

prod_db  = firestore.client(app=_prod_app)
orbit_db = firestore.client(app=_orbit_app)
orbit_bucket = storage.bucket(app=_orbit_app)

# ─── API Clients ──────────────────────────────────────────────────────────────
oai_client = openai.OpenAI(api_key=OPENAI_API_KEY)


# ══════════════════════════════════════════════════════════════════════════════
# PRODUCTION DB READERS
# ══════════════════════════════════════════════════════════════════════════════

def fetch_course(course_id: str) -> dict:
    """Read a course document from the production Firestore."""
    doc = prod_db.collection("Courses").document(course_id).get()
    if not doc.exists:
        raise ValueError(f"Course {course_id} not found in production DB")
    return {"id": doc.id, **doc.to_dict()}


def fetch_lesson(lesson_id: str) -> dict:
    """Read a lesson document from the production Firestore."""
    doc = prod_db.collection("Lessons").document(lesson_id).get()
    if not doc.exists:
        raise ValueError(f"Lesson {lesson_id} not found in production DB")
    return {"id": doc.id, **doc.to_dict()}


GROUPING_SYSTEM = """
You are helping migrate course content from one platform to another.

In the source platform, a single logical "lesson" is split into multiple separate
items of different types — a video lecture, Miro board notes, and/or one or more
Colab notebooks for the same topic are stored as individual items.

Your job: group these items into logical Orbit lessons.

Rules:
- Each group represents one lesson in the new platform
- Each group has at most one video and one Miro board, but may have multiple Colab items
- The video is always the primary item — every group should ideally have a video
- If a Miro or Colab item clearly belongs to a specific video (same topic, adjacent
  position, matching keywords in titles), pair them in the same group
- Multiple Colab items that cover the same lesson topic should be grouped together
- If a Miro or Colab item has no clear video pair, give it its own group with
  videoId set to null
- Preserve curriculum order — groups should appear in the same order as the source items
- Use the source item titles to produce a clean lesson title for each group

Return ONLY a valid JSON array — no prose:
[
  {
    "title": "Clean lesson title",
    "videoId": "id_string or null",
    "miroId": "id_string or null",
    "colabIds": ["id_string", ...]
  }
]
"""

def group_topic_items(topic: dict) -> list[dict]:
    """
    Returns an ordered list of lesson groups for a single topic.
    Each group: { title, videoId, miroId, colabIds, topicId, topicTitle }
    Uses an LLM to cluster production DB items (which may be split by type)
    into logical Orbit lessons.
    Only items with type == LESSON_TYPE are considered.
    """
    items = [it for it in topic.get("items", []) if it.get("type") == LESSON_TYPE]
    topic_id = topic["id"]
    topic_title = topic.get("title", "")

    if not items:
        return []

    # Single item — treat it as the sole video lesson, no LLM needed
    if len(items) == 1:
        item = items[0]
        return [{
            "title": item.get("title", topic_title),
            "videoId": item["id"],
            "miroId": None,
            "colabIds": [],
            "topicId": topic_id,
            "topicTitle": topic_title,
        }]

    # Build a compact representation for the LLM
    items_repr = json.dumps([
        {"id": it["id"], "type": it.get("type", ""), "title": it.get("title", "")}
        for it in items
    ])
    user = f'Topic: "{topic_title}"\n\nItems:\n{items_repr}'

    try:
        groups = _llm_json_array(GROUPING_SYSTEM, user, max_tokens=1024)
        for g in groups:
            g["topicId"] = topic_id
            g["topicTitle"] = topic_title
            # Normalise: LLM may return colabId (singular) — convert defensively
            if "colabId" in g and "colabIds" not in g:
                g["colabIds"] = [g.pop("colabId")] if g["colabId"] else []
            g.setdefault("colabIds", [])
        return groups
    except Exception as e:
        print(f"  [WARN] Grouping LLM failed for topic '{topic_title}': {e}")
        # Fallback: treat each item as its own solo lesson
        return [
            {
                "title": item.get("title", topic_title),
                "videoId": item["id"],
                "miroId": None,
                "colabIds": [],
                "topicId": topic_id,
                "topicTitle": topic_title,
            }
            for item in items
        ]


# Only items with this type on the topic item are processed
LESSON_TYPE = "LESSON"

# URLs that are never downloadable video sources — filtered at all stages
_IGNORED_URL_PATTERNS = re.compile(
    r'(calendar\.google\.com|discord\.(gg|com)|senja\.io|zoom\.us|veed\.io)',
    re.IGNORECASE
)


def _extract_url_from_description(description: str) -> str:
    """
    Scan a lesson description for a usable video URL.
    Ignores calendar invites, Discord invites, Senja links, and Zoom meeting links.
    Returns the first valid URL found, or an empty string.
    """
    for match in re.finditer(r'https?://\S+', description or ""):
        url = match.group().rstrip(".,;)")
        if not _IGNORED_URL_PATTERNS.search(url):
            return url
    return ""


def embed_to_video_url(embed: str) -> str:
    """
    Convert an embed URL to a URL yt-dlp can download.
    Handles YouTube and Vimeo embed formats.
    Falls back to the original URL (yt-dlp supports many formats).
    """
    if not embed:
        return ""
    yt_match = re.search(r'youtube\.com/embed/([^?&/]+)', embed)
    if yt_match:
        return f"https://www.youtube.com/watch?v={yt_match.group(1)}"
    vimeo_match = re.search(r'vimeo\.com/video/(\d+)', embed)
    if vimeo_match:
        return f"https://vimeo.com/{vimeo_match.group(1)}"
    return embed


def resolve_group_urls(group: dict) -> dict:
    """
    Fetch the individual lesson docs for each item in the group and resolve:
      - videoUrl     (from video lesson's embedUrl, converted to downloadable URL)
      - embedUrl     (original embed URL, preserved)
      - miroBoardUrl (from miro lesson's embedUrl — kept as-is for iframe embedding)
      - colabUrls    (list of plain Colab URLs — one per colabId; open in new tab, never embedded)
    Returns a dict of resolved URLs.
    """
    result = {"videoUrl": None, "embedUrl": None, "miroBoardUrl": None, "colabUrls": []}

    if group.get("videoId"):
        doc = prod_db.collection("Lessons").document(group["videoId"]).get()
        if doc.exists:
            data = doc.to_dict()
            if data.get("type") == "VIDEO LECTURE":
                raw = data.get("embedUrl", "")
                embed = raw if raw and not _IGNORED_URL_PATTERNS.search(raw) else ""
            else:
                embed = _extract_url_from_description(data.get("description", ""))
            result["embedUrl"] = embed
            result["videoUrl"] = embed_to_video_url(embed)

    if group.get("miroId"):
        doc = prod_db.collection("Lessons").document(group["miroId"]).get()
        if doc.exists:
            data = doc.to_dict()
            # Miro embed URL — kept as-is (embedded via iframe in the MIRO NOTES tab)
            if data.get("type") == "MIRO BOARD":
                result["miroBoardUrl"] = data.get("embedUrl")

    for colab_id in group.get("colabIds", []):
        doc = prod_db.collection("Lessons").document(colab_id).get()
        if doc.exists:
            data = doc.to_dict()
            # Colab is a plain link — NOT embedded. Use the raw URL.
            url = (
                data.get("externalToolLink") or data.get("embedUrl") or data.get("colabUrl")
            )
            if url:
                result["colabUrls"].append(url)

    return result


# ══════════════════════════════════════════════════════════════════════════════
# PIPELINE STATE (written to Orbit Firestore)
# ══════════════════════════════════════════════════════════════════════════════

def get_course_state(course_id: str) -> dict:
    doc = orbit_db.collection("_PipelineState").document(course_id).get()
    return doc.to_dict() if doc.exists else {}


def set_course_state(course_id: str, data: dict):
    orbit_db.collection("_PipelineState").document(course_id).set(
        {**data, "updatedAt": SERVER_TIMESTAMP}, merge=True
    )


def get_lesson_state(course_id: str, lesson_id: str) -> dict:
    doc = (orbit_db.collection("_PipelineState")
                   .document(course_id)
                   .collection("Lessons")
                   .document(lesson_id)
                   .get())
    return doc.to_dict() if doc.exists else {}


def set_lesson_state(course_id: str, lesson_id: str, data: dict):
    (orbit_db.collection("_PipelineState")
             .document(course_id)
             .collection("Lessons")
             .document(lesson_id)
             .set({**data, "updatedAt": SERVER_TIMESTAMP}, merge=True))


def mark_step_done(course_id: str, lesson_id: str, step: str):
    state = get_lesson_state(course_id, lesson_id)
    done = list(set(state.get("stepsCompleted", []) + [step]))
    set_lesson_state(course_id, lesson_id, {"stepsCompleted": done})


# ══════════════════════════════════════════════════════════════════════════════
# STEP 1: TRANSCRIPTION
# ══════════════════════════════════════════════════════════════════════════════

def transcribe(video_url: str) -> dict:
    with tempfile.TemporaryDirectory() as tmpdir:
        audio_path = _download_audio(video_url, tmpdir)
        return _transcribe_audio(audio_path)


def _download_audio(url: str, output_dir: str) -> str:
    output_path = os.path.join(output_dir, "audio.mp3")
    if _is_direct_file(url):
        import urllib.request
        urllib.request.urlretrieve(url, output_path)
    else:
        cookies_file = Path(__file__).parent / "yt-cookies.txt"
        cmd = [YT_DLP, "--extract-audio", "--audio-format", "mp3",
               "--audio-quality", "0", "--output", output_path, "--no-playlist",
               "--remote-components", "ejs:github"]
        if cookies_file.exists():
            cmd += ["--cookies", str(cookies_file)]
        cmd.append(url)
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode != 0:
            raise RuntimeError(f"yt-dlp failed: {result.stderr[:500]}")
    return output_path


def _is_direct_file(url: str) -> bool:
    return any(url.lower().endswith(ext) for ext in (".mp4", ".mkv", ".webm", ".m4a", ".mp3"))


def _transcribe_audio(audio_path: str) -> dict:
    size_mb = Path(audio_path).stat().st_size / (1024 * 1024)
    if size_mb > 24:
        return _transcribe_chunked(audio_path)
    return _transcribe_single(audio_path)


def _transcribe_single(audio_path: str) -> dict:
    with open(audio_path, "rb") as f:
        resp = oai_client.audio.transcriptions.create(
            model="whisper-1",
            file=f,
            response_format="verbose_json",
            timestamp_granularities=["segment"]
        )
    segments = [{"start": s.start, "end": s.end, "text": s.text.strip()}
                for s in resp.segments if s.text.strip()]
    return {"segments": segments, "fullText": " ".join(s["text"] for s in segments)}


def _transcribe_chunked(audio_path: str) -> dict:
    chunk_dir = Path(audio_path).parent / "chunks"
    chunk_dir.mkdir(exist_ok=True)
    subprocess.run(
        ["ffmpeg", "-i", audio_path, "-f", "segment", "-segment_time", "600",
         "-c", "copy", str(chunk_dir / "chunk_%03d.mp3")],
        check=True, capture_output=True
    )
    all_segments, offset = [], 0.0
    for chunk in sorted(chunk_dir.glob("chunk_*.mp3")):
        result = _transcribe_single(str(chunk))
        for seg in result["segments"]:
            all_segments.append({**seg, "start": seg["start"] + offset, "end": seg["end"] + offset})
        if result["segments"]:
            offset = all_segments[-1]["end"]
    return {"segments": all_segments, "fullText": " ".join(s["text"] for s in all_segments)}


# ══════════════════════════════════════════════════════════════════════════════
# STEP 2: LLM EXTRACTION
# ══════════════════════════════════════════════════════════════════════════════

EXTRACTION_SYSTEM = """
You are an expert educational content analyst. Given a lecture transcript, extract structured metadata.
Return ONLY valid JSON matching this schema exactly — no prose, no markdown fences:
{
  "shortDescription": "2-3 sentence overview",
  "keyConcepts": ["string"],
  "learningOutcomes": ["string — start with action verbs"],
  "chapterMarkers": [{"timestamp": float, "label": "string"}],
  "difficulty": "beginner" | "intermediate" | "advanced",
  "prerequisites": ["string from controlled vocabulary"],
  "estimatedDurationHours": float
}
Controlled prerequisite vocabulary (use only these):
linear-algebra, calculus, probability, statistics, python-basics, python-numpy,
pytorch, tensorflow, ml-foundations, deep-learning-basics, cnn-basics, rnn-basics,
attention-mechanism, transformer-architecture, data-structures, algorithms,
computer-vision-basics, nlp-basics, reinforcement-learning-basics
"""

def llm_extract(transcript: dict, lesson: dict) -> dict:
    full_text = transcript["fullText"][:MAX_TRANSCRIPT_CHARS]
    user = f"Lesson title: {lesson['title']}\n\nTranscript:\n{full_text}"
    return _llm_json(EXTRACTION_SYSTEM, user, max_tokens=2048)


# ══════════════════════════════════════════════════════════════════════════════
# STEP 3: SUMMARY GENERATION
# ══════════════════════════════════════════════════════════════════════════════

SUMMARY_SYSTEM = """
You are an expert technical writer. Write a comprehensive long-form lesson summary
from the lecture transcript and metadata provided.

Requirements:
- Cover all key concepts with clear prose explanations
- Define new terms on first use
- Include important formulas in LaTeX notation where helpful
- Structure sections around the chapter markers (use them as ## headings)
- At points where a diagram would significantly aid understanding, insert:
  [FIGURE: "precise, visual description for an image generation API"]
  (on its own line, 2-6 figures total, distributed throughout)

Return ONLY the summary text — no preamble, no JSON. Start with the first ## heading.
"""

def generate_summary(transcript: dict, extraction: dict, lesson_title: str) -> dict:
    markers = "\n".join(
        f"  {_fmt_ts(m['timestamp'])}: {m['label']}"
        for m in extraction.get("chapterMarkers", [])
    )
    full_text = transcript["fullText"][:MAX_TRANSCRIPT_CHARS]
    user = (
        f"Lesson title: {lesson_title}\n"
        f"Key concepts: {', '.join(extraction.get('keyConcepts', []))}\n"
        f"Learning outcomes:\n" +
        "\n".join(f"- {o}" for o in extraction.get("learningOutcomes", [])) +
        f"\n\nChapter markers:\n{markers}\n\nTranscript:\n{full_text}"
    )
    response = _llm_raw(SUMMARY_SYSTEM, user, max_tokens=4096)
    return {"contentRaw": response, "figureCount": response.count("[FIGURE:")}


def _fmt_ts(seconds: float) -> str:
    m, s = divmod(int(seconds), 60)
    h, m = divmod(m, 60)
    return f"{h:02d}:{m:02d}:{s:02d}" if h else f"{m:02d}:{s:02d}"


# ══════════════════════════════════════════════════════════════════════════════
# STEP 4: FIGURE GENERATION (PaperBanana)
# ══════════════════════════════════════════════════════════════════════════════

def generate_figures(course_id: str, lesson_id: str, summary: dict) -> dict:
    content = summary["contentRaw"]
    seen = {}
    index = [0]

    def replace(match):
        description = match.group(1)
        if description in seen:
            return seen[description]
        try:
            url = _paperbanana_and_upload(description, course_id, lesson_id, index[0])
            md = f"![{description[:60]}]({url})"
            seen[description] = md
            index[0] += 1
            return md
        except Exception as e:
            print(f"  [WARN] Figure {index[0]} failed: {e}")
            index[0] += 1
            return f"<!-- figure failed: {description[:80]} -->"

    final = FIGURE_PATTERN.sub(replace, content)
    return {"content": final, "contentRaw": content, "figureCount": len(seen)}


def _paperbanana_and_upload(description: str, course_id: str, lesson_id: str, index: int) -> str:
    import asyncio
    from paperbanana import DiagramType, GenerationInput, PaperBananaPipeline
    from paperbanana.core.config import Settings

    if not GOOGLE_API_KEY:
        raise RuntimeError("GOOGLE_API_KEY not set")

    settings = Settings(
        vlm_provider="gemini",
        vlm_model="gemini-2.0-flash",
        image_provider="google_imagen",
        image_model="gemini-3-pro-image-preview",
        refinement_iterations=3,
    )
    pipeline = PaperBananaPipeline(settings=settings)
    result = asyncio.run(pipeline.generate(
        GenerationInput(
            source_context=FIGURE_STYLE_CONTEXT + "\n\nDiagram to generate:\n" + description,
            communicative_intent=description,
            diagram_type=DiagramType.METHODOLOGY,
        )
    ))

    storage_path = f"lesson_figures/{course_id}/{lesson_id}/figure_{index:03d}.png"
    blob = orbit_bucket.blob(storage_path)
    blob.upload_from_filename(result.image_path, content_type="image/png")
    blob.make_public()
    return blob.public_url


# ══════════════════════════════════════════════════════════════════════════════
# STEP 5: MANDATORY QUIZ GENERATION
# ══════════════════════════════════════════════════════════════════════════════

QUIZ_SYSTEM = """
You are an expert educator creating quiz questions for an online platform.
Given transcripts and key concepts from all lessons in a course topic, generate exactly 20 questions.

Rules:
- Draw from ALL lessons provided (distribute questions proportionally across lessons)
- Mix: 10 multiple_choice, 10 true_false
- multiple_choice distractors must be plausible — not obviously wrong
- Tag each question to its primary concept and lesson

Return ONLY a valid JSON array of 20 objects:
[{
  "id": "q_<8-char hex>",
  "type": "multiple_choice" | "true_false",
  "text": "question text",
  "options": ["A","B","C","D"] | null,
  "correctIndex": 0|1|2|3 | null,
  "conceptTags": ["concept"],
  "lessonId": "lesson_id"
}]

For true_false questions, set options to ["True", "False"] and correctIndex to 0 or 1.
"""

def generate_quiz(course_id: str, topic_id: str, lesson_ids: list) -> dict:
    """Generate a mandatory quiz for a topic, covering all its STANDARD lessons."""
    parts = []
    for i, lid in enumerate(lesson_ids):
        t = orbit_db.collection("Transcripts").document(f"{course_id}_{lid}").get().to_dict()
        l = orbit_db.collection("Lessons").document(lid).get().to_dict()
        full_text = (t or {}).get("fullText", "")[:MAX_CHARS_PER_LESSON]
        concepts = ", ".join((l or {}).get("keyConcepts", []))
        title = (l or {}).get("title", lid)
        parts.append(
            f"--- Lesson {i+1}: {title} (ID: {lid}) ---\n"
            f"Key concepts: {concepts}\n\nTranscript:\n{full_text}"
        )

    user = f"Topic ID: {topic_id} — {len(lesson_ids)} lesson(s)\n\n" + "\n\n".join(parts)
    questions = _llm_json_array(QUIZ_SYSTEM, user, max_tokens=4096)

    # Ensure IDs exist
    for q in questions:
        if not q.get("id"):
            q["id"] = f"q_{uuid.uuid4().hex[:8]}"

    return {"topicId": topic_id, "sourceLessonIds": lesson_ids, "questions": questions}


# ══════════════════════════════════════════════════════════════════════════════
# STEP 6: COURSE-LEVEL AGGREGATION
# ══════════════════════════════════════════════════════════════════════════════

AGGREGATION_SYSTEM = """
You are a curriculum designer. Given metadata from all lessons in a course,
synthesize a course-level record.

Return ONLY valid JSON:
{
  "shortDescription": "2-3 sentence teaser for the course catalog",
  "prerequisites": ["controlled-vocab-string"],
  "difficulty": "beginner" | "intermediate" | "advanced",
  "topicsCovered": ["deduplicated concept tags, max 30"]
}
"""

def aggregate_course(course: dict, orbit_topics: list) -> dict:
    """
    Reads lesson docs from the Orbit Lessons collection (written by write_lesson_to_orbit)
    to synthesize course-level fields. Updates the existing course document in-place —
    all production DB fields are preserved; only Orbit pipeline fields are set/overwritten.

    orbit_topics: list of { id, title, lessonIds } built by run_course.
    """
    parts = []
    total_hours = 0.0
    all_lesson_ids = [lid for t in orbit_topics for lid in t["lessonIds"] if not lid.startswith("quiz_")]

    for lid in all_lesson_ids:
        l = orbit_db.collection("Lessons").document(lid).get()
        if not l.exists:
            continue
        d = l.to_dict()
        total_hours += d.get("estimatedDurationHours", 0)
        parts.append(
            f"Lesson: {d.get('title','')}\n"
            f"  Description: {d.get('shortDescription','')}\n"
            f"  Key concepts: {', '.join(d.get('keyConcepts', []))}\n"
            f"  Difficulty: {d.get('difficulty','')}"
        )

    user = f"Course title: {course['title']}\n\nLessons:\n" + "\n\n".join(parts)
    data = _llm_json(AGGREGATION_SYSTEM, user, max_tokens=1024)

    # Merge into existing course document — do not overwrite production DB fields
    return {
        "topics": orbit_topics,                          # replaces Topic[] with OrbitTopic[]
        "shortDescription": data.get("shortDescription", ""),
        "prerequisites": data.get("prerequisites", []),
        "difficulty": data.get("difficulty", ""),
        "topicsCovered": data.get("topicsCovered", []),
        "estimatedDurationHours": round(total_hours, 1),
        "updatedAt": SERVER_TIMESTAMP,
    }


# ══════════════════════════════════════════════════════════════════════════════
# FIRESTORE WRITERS
# ══════════════════════════════════════════════════════════════════════════════

def write_transcript(course_id: str, lesson_id: str, transcript: dict):
    orbit_db.collection("Transcripts").document(f"{course_id}_{lesson_id}").set({
        "courseId": course_id,
        "lessonId": lesson_id,
        **transcript,
        "createdAt": SERVER_TIMESTAMP,
    })


def write_summary(course_id: str, lesson_id: str, summary: dict):
    orbit_db.collection("LessonSummaries").document(f"{course_id}_{lesson_id}").set({
        "courseId": course_id,
        "lessonId": lesson_id,
        "content": summary.get("content", summary.get("contentRaw", "")),
        "contentRaw": summary.get("contentRaw", ""),
        "figureCount": summary.get("figureCount", 0),
        "createdAt": SERVER_TIMESTAMP,
    })


def write_lesson_to_orbit(course_id: str, topic_id: str, group: dict, urls: dict, video_lesson: dict, extraction: dict):
    """
    Write the merged Orbit lesson document to courses/{courseId}/lessons/{lessonId}.
    `topic_id`    — parent OrbitTopic id
    `group`       — LLM-produced group { title, videoId, miroId, colabIds }
    `urls`        — resolved URLs from resolve_group_urls()
    `video_lesson`— the primary video lesson document from production DB
    `extraction`  — Step 2 LLM extraction output
    Written to top-level Lessons collection (not a subcollection of Courses).
    """
    lesson_id = group["videoId"]  # Orbit lesson ID = primary video lesson ID
    orbit_db.collection("Lessons").document(lesson_id).set({
        "id": lesson_id,
        "courseId": course_id,
        "topicId": topic_id,
        "title": group.get("title") or video_lesson.get("title", ""),
        "type": "STANDARD",
        "description": video_lesson.get("description", ""),
        # Content URLs
        "videoUrl": urls["videoUrl"],
        "embedUrl": urls["embedUrl"],
        "miroBoardUrl": urls["miroBoardUrl"],    # iframe-embeddable Miro URL
        "colabUrls": urls["colabUrls"],          # plain links — each opens in new tab
        "duration": video_lesson.get("duration"),
        "durationAddedToLearningProgress": True,
        # Pipeline-generated metadata (note: prerequisites NOT written to lesson doc)
        "shortDescription": extraction.get("shortDescription", ""),
        "keyConcepts": extraction.get("keyConcepts", []),
        "learningOutcomes": extraction.get("learningOutcomes", []),
        "chapterMarkers": extraction.get("chapterMarkers", []),
        "difficulty": extraction.get("difficulty", ""),
        "estimatedDurationHours": extraction.get("estimatedDurationHours", 0),
        "createdAt": SERVER_TIMESTAMP,
        "updatedAt": SERVER_TIMESTAMP,
    })


def write_quiz(course_id: str, topic_id: str, quiz: dict):
    """
    Write the mandatory quiz document and a corresponding MANDATORY_QUIZ lesson
    stub so the curriculum sidebar can render it as a node in the topic's lessonIds.
    """
    quiz_lesson_id = f"quiz_{topic_id}"

    # Quiz document: Quizzes/mandatory_{topicId}
    orbit_db.collection("Quizzes").document(f"mandatory_{topic_id}").set({
        "id": f"mandatory_{topic_id}",
        "courseId": course_id,
        "topicId": topic_id,
        "sourceLessonIds": quiz["sourceLessonIds"],
        "questions": quiz["questions"],
        "createdAt": SERVER_TIMESTAMP,
    })

    # MANDATORY QUIZ lesson stub: Lessons/quiz_{topicId}
    # This is the node that appears at the end of the topic in lessonIds.
    orbit_db.collection("Lessons").document(quiz_lesson_id).set({
        "id": quiz_lesson_id,
        "courseId": course_id,
        "topicId": topic_id,
        "title": "Topic Quiz",
        "description": "",
        "type": "MANDATORY QUIZ",
        "duration": {"hours": 0, "minutes": 0},
        "durationAddedToLearningProgress": False,
        "createdAt": SERVER_TIMESTAMP,
        "updatedAt": SERVER_TIMESTAMP,
    })


def seed_course(course: dict):
    """
    Copy base fields from the production course document into Orbit Firestore.
    Uses merge=True so re-runs are idempotent and never overwrite pipeline-written fields.
    `topics` is intentionally excluded — the pipeline builds its own OrbitTopic[] structure.
    """
    FIELDS = [
        "title", "slug", "description", "duration", "thumbnail",
        "regularPrice", "salePrice", "pricingModel", "subscriptionPlans",
        "categoryIds", "targetAudienceIds", "tags",
        "instructorId", "instructorName",
        "status", "mode", "liveAt",
        "certificateTemplateId",
        "isEnrollmentPaused", "isMailSendingEnabled",
        "isCertificateEnabled", "isCourseCompletionEnabled", "customCertificateName",
        "isForumEnabled", "isWelcomeMessageEnabled",
        "externalToolLink",
        "createdAt",
    ]
    doc = {k: course[k] for k in FIELDS if k in course}
    doc["id"] = course["id"]
    orbit_db.collection("Courses").document(course["id"]).set(doc, merge=True)


def write_course(course_id: str, orbit_fields: dict):
    """Merge Orbit pipeline fields into the existing course document."""
    orbit_db.collection("Courses").document(course_id).set(orbit_fields, merge=True)


# ══════════════════════════════════════════════════════════════════════════════
# LESSON PROCESSOR
# ══════════════════════════════════════════════════════════════════════════════

def process_lesson(course_id: str, topic_id: str, group: dict):
    """
    Process one lesson group (video + optional miro + optional colabs).
    The group's videoId is used as the Orbit lesson ID.
    If the group has no videoId (miro/colab-only), it is skipped with a warning.
    Returns the extraction dict on success, None if skipped.
    """
    lesson_id = group.get("videoId")
    if not lesson_id:
        print(f"  → Skipping group '{group.get('title', '')}' — no video")
        return None

    state = get_lesson_state(course_id, lesson_id)
    done = set(state.get("stepsCompleted", []))

    colab_count = len(group.get("colabIds", []))
    print(f"  → Lesson: {group.get('title', lesson_id)}"
          + (f" [+Miro]" if group.get("miroId") else "")
          + (f" [+{colab_count} Colab]" if colab_count else ""))

    # Resolve all URLs from the production DB lesson documents
    urls = resolve_group_urls(group)
    if not urls["videoUrl"]:
        print(f"  [WARN] Could not resolve video URL for lesson {lesson_id}, skipping")
        return None

    # Fetch the primary video lesson doc for metadata fields
    video_lesson = fetch_lesson(lesson_id)

    # ── Step 1: Transcription ────────────────────────────────────────────────
    if "transcription" not in done:
        print("    [1/4] Transcribing...")
        set_lesson_state(course_id, lesson_id, {"status": "transcribing"})
        transcript = transcribe(urls["videoUrl"])
        write_transcript(course_id, lesson_id, transcript)
        mark_step_done(course_id, lesson_id, "transcription")
    else:
        print("    [1/4] Transcription already done, loading...")
        transcript = orbit_db.collection("Transcripts").document(f"{course_id}_{lesson_id}").get().to_dict()

    # ── Step 2: LLM Extraction ───────────────────────────────────────────────
    if "extraction" not in done:
        print("    [2/4] Extracting metadata...")
        set_lesson_state(course_id, lesson_id, {"status": "extracting"})
        lesson_meta = {**video_lesson, "title": group.get("title") or video_lesson.get("title", "")}
        extraction = llm_extract(transcript, lesson_meta)
        # Write the lesson document — this is the single source of truth for all metadata
        write_lesson_to_orbit(course_id, topic_id, group, urls, video_lesson, extraction)
        mark_step_done(course_id, lesson_id, "extraction")
    else:
        print("    [2/4] Extraction already done, loading...")
        extraction = orbit_db.collection("Lessons").document(lesson_id).get().to_dict()

    # ── Step 3: Summary Generation ───────────────────────────────────────────
    if "summary" not in done:
        print("    [3/4] Generating summary...")
        set_lesson_state(course_id, lesson_id, {"status": "summarizing"})
        lesson_title = group.get("title") or video_lesson.get("title", "")
        summary = generate_summary(transcript, extraction, lesson_title)
        mark_step_done(course_id, lesson_id, "summary")
    else:
        print("    [3/4] Summary already done, loading...")
        saved = orbit_db.collection("LessonSummaries").document(f"{course_id}_{lesson_id}").get().to_dict()
        summary = {"contentRaw": saved.get("contentRaw", ""), "figureCount": saved.get("figureCount", 0)}

    # ── Step 4: Figure Generation ────────────────────────────────────────────
    if "figures" not in done:
        print(f"    [4/4] Generating {summary['figureCount']} figures...")
        set_lesson_state(course_id, lesson_id, {"status": "figures"})
        final_summary = generate_figures(course_id, lesson_id, summary)
        write_summary(course_id, lesson_id, final_summary)
        mark_step_done(course_id, lesson_id, "figures")
    else:
        print("    [4/4] Figures already done.")

    set_lesson_state(course_id, lesson_id, {"status": "done"})
    print(f"    ✓ Lesson complete")
    return extraction


# ══════════════════════════════════════════════════════════════════════════════
# COURSE ORCHESTRATOR
# ══════════════════════════════════════════════════════════════════════════════

def run_course(course_id: str):
    print(f"\n{'='*60}")
    print(f"Processing course: {course_id}")
    print(f"{'='*60}")

    course_state = get_course_state(course_id)
    if course_state.get("status") == "done":
        print(f"  Course already complete. Skipping.")
        return

    course = fetch_course(course_id)
    topics = course.get("topics", [])

    if not topics:
        print(f"  [WARN] No topics found in course {course_id}")
        return

    print(f"  Found {len(topics)} topic(s)")
    seed_course(course)
    print(f"  ✓ Course document seeded in Orbit")
    set_course_state(course_id, {"status": "processing"})

    orbit_topics = []  # Built up per topic; written to course doc in aggregation

    for topic in topics:
        topic_id = topic["id"]
        topic_title = topic.get("title", topic_id)
        quiz_state_key = f"quiz_topic_{topic_id}"

        print(f"\n  ── Topic: {topic_title} ──")

        # ── Group this topic's items into Orbit lessons ───────────────────────
        groups = group_topic_items(topic)
        if not groups:
            print(f"    [WARN] No processable groups in topic '{topic_title}', skipping")
            continue

        topic_lesson_ids = []  # STANDARD lesson IDs for this topic

        for i, group in enumerate(groups):
            lesson_id = group.get("videoId")
            if not lesson_id:
                print(f"    → Group {i+1}/{len(groups)}: '{group.get('title','')}' has no video, skipping")
                continue

            lesson_state = get_lesson_state(course_id, lesson_id)
            if lesson_state.get("status") == "done":
                print(f"    → Lesson {i+1}/{len(groups)} already done, skipping")
                topic_lesson_ids.append(lesson_id)
                continue

            try:
                result = process_lesson(course_id, topic_id, group)
                if result is not None:
                    topic_lesson_ids.append(lesson_id)
            except Exception as e:
                print(f"    [ERROR] Lesson {lesson_id} failed: {e}")
                traceback.print_exc()
                set_lesson_state(course_id, lesson_id, {"status": "failed", "error": str(e)})
                set_course_state(course_id, {"status": "failed", "failedAt": lesson_id})
                raise  # stop the course run — fix and re-run

        if not topic_lesson_ids:
            print(f"    [WARN] No lessons processed for topic '{topic_title}', skipping quiz")
            continue

        # ── Generate mandatory quiz for this topic ────────────────────────────
        quiz_lesson_id = f"quiz_{topic_id}"

        if course_state.get(quiz_state_key) != "done":
            print(f"\n    [QUIZ] Generating mandatory quiz for topic '{topic_title}'...")
            try:
                quiz = generate_quiz(course_id, topic_id, topic_lesson_ids)
                write_quiz(course_id, topic_id, quiz)
                set_course_state(course_id, {quiz_state_key: "done"})
                print(f"    [QUIZ] ✓ Quiz written ({len(quiz['questions'])} questions)")
            except Exception as e:
                print(f"    [WARN] Quiz generation for topic '{topic_title}' failed: {e}")
        else:
            print(f"    [QUIZ] Quiz for topic '{topic_title}' already done, skipping")

        # lessonIds = STANDARD lessons + MANDATORY_QUIZ node (always last)
        orbit_topics.append({
            "id": topic_id,
            "title": topic_title,
            "lessonIds": topic_lesson_ids + [quiz_lesson_id],
        })

    # ── Course-level aggregation ──────────────────────────────────────────────
    print(f"\n  [AGGREGATION] Synthesising course-level fields...")
    orbit_fields = aggregate_course(course, orbit_topics)
    write_course(course_id, orbit_fields)

    total_lessons = sum(len(t["lessonIds"]) - 1 for t in orbit_topics)  # exclude quiz nodes
    set_course_state(course_id, {"status": "done"})
    print(f"\n  ✓ Course {course_id} complete ({total_lessons} lessons across {len(orbit_topics)} topics)")


# ══════════════════════════════════════════════════════════════════════════════
# LLM HELPERS
# ══════════════════════════════════════════════════════════════════════════════

def _llm_json(system: str, user: str, max_tokens: int) -> dict:
    raw = _llm_raw(system, user, max_tokens)
    return _parse_json_object(raw)


def _llm_json_array(system: str, user: str, max_tokens: int) -> list:
    raw = _llm_raw(system, user, max_tokens)
    return _parse_json_array(raw)


def _llm_raw(system: str, user: str, max_tokens: int) -> str:
    for attempt in range(3):
        try:
            msg = oai_client.chat.completions.create(
                model=LLM_MODEL,
                max_tokens=max_tokens,
                messages=[
                    {"role": "system", "content": system},
                    {"role": "user", "content": user},
                ]
            )
            return msg.choices[0].message.content
        except Exception as e:
            if attempt == 2:
                raise
            time.sleep(2 ** attempt)


def _parse_json_object(text: str) -> dict:
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        match = re.search(r'\{.*\}', text, re.DOTALL)
        if match:
            return json.loads(match.group())
        raise ValueError(f"No JSON object found in LLM response: {text[:200]}")


def _parse_json_array(text: str) -> list:
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        match = re.search(r'\[.*\]', text, re.DOTALL)
        if match:
            return json.loads(match.group())
        raise ValueError(f"No JSON array found in LLM response: {text[:200]}")


# ══════════════════════════════════════════════════════════════════════════════
# ENTRY POINT
# ══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    course_ids = sys.argv[1:] if len(sys.argv) > 1 else COURSE_IDS

    if not course_ids:
        print("No course IDs provided. Add them to COURSE_IDS in this script or pass as args.")
        sys.exit(1)

    print(f"Starting pipeline for {len(course_ids)} course(s)")
    failed = []

    for cid in course_ids:
        try:
            run_course(cid)
        except Exception as e:
            print(f"\n[FATAL] Course {cid} failed: {e}")
            failed.append(cid)
            continue  # move to the next course

    print(f"\n{'='*60}")
    print(f"Pipeline complete. {len(course_ids) - len(failed)}/{len(course_ids)} courses succeeded.")
    if failed:
        print(f"Failed courses: {failed}")
