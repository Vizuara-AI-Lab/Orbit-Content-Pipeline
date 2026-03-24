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
MAX_AUDIT_CHARS       = 60_000   # ~15k tokens — keeps output within gpt-4o's 16k limit
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
- Spelling is absolutely non-negotiable. Every word, label, and annotation must be spelled
  correctly. A spelling mistake renders the diagram unusable.
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
- Each group has at most one video, but may have multiple Miro boards and multiple Colab items
- The video is the primary item — every group should ideally have a video
- If Miro boards or Colab items clearly belong to a specific video (same topic, adjacent
  position, matching keywords in titles), pair them in the same group
- Multiple Miro boards covering the same lesson topic should be grouped together
- Multiple Colab items that cover the same lesson topic should be grouped together
- If Miro boards or Colab items have no clear video pair, give them their own group with
  videoId set to null — this creates a standalone notes/notebook lesson with no video
- Each item includes a description that may contain the actual URL — a miro.com URL means Miro,
  a colab.research.google.com URL means Colab, a YouTube/Vimeo URL means video. Prefer this
  signal over the type field when they disagree
- Preserve curriculum order — groups should appear in the same order as the source items
- Use the source item titles to produce a clean lesson title for each group

Return ONLY a valid JSON array — no prose:
[
  {
    "title": "Clean lesson title",
    "videoId": "id_string or null",
    "miroIds": ["id_string", ...],
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

    # Single item — treat it as its own lesson, no LLM needed
    if len(items) == 1:
        item = items[0]
        kind = _classify_item(item)
        return [{
            "title": item.get("title", topic_title),
            "videoId": item["id"] if kind == "video" else None,
            "miroIds": [item["id"]] if kind == "miro" else [],
            "colabIds": [item["id"]] if kind == "colab" else [],
            "topicId": topic_id,
            "topicTitle": topic_title,
        }]

    # Build a compact representation for the LLM
    items_repr = json.dumps([
        {
            "id": it["id"],
            "type": it.get("type", ""),
            "title": it.get("title", ""),
            "description": it.get("description") or "",
        }
        for it in items
    ])
    user = f'Topic: "{topic_title}"\n\nItems:\n{items_repr}'

    try:
        groups = _llm_json_array(GROUPING_SYSTEM, user, max_tokens=1024)
        for g in groups:
            g["topicId"] = topic_id
            g["topicTitle"] = topic_title
            # Normalise: LLM may return singular forms — convert defensively
            if "colabId" in g and "colabIds" not in g:
                g["colabIds"] = [g.pop("colabId")] if g["colabId"] else []
            g.setdefault("colabIds", [])
            if "miroId" in g and "miroIds" not in g:
                g["miroIds"] = [g.pop("miroId")] if g["miroId"] else []
            g.setdefault("miroIds", [])
        return groups
    except Exception as e:
        print(f"  [WARN] Grouping LLM failed for topic '{topic_title}': {e}")
        # Fallback: treat each item as its own solo lesson
        return [
            {
                "title": item.get("title", topic_title),
                "videoId": item["id"] if _classify_item(item) == "video" else None,
                "miroIds": [item["id"]] if _classify_item(item) == "miro" else [],
                "colabIds": [item["id"]] if _classify_item(item) == "colab" else [],
                "topicId": topic_id,
                "topicTitle": topic_title,
            }
            for item in items
        ]


# Only items with this type on the topic item are processed
LESSON_TYPE = "LESSON"

# URLs that are never downloadable video sources — filtered at all stages
_IGNORED_URL_PATTERNS = re.compile(
    r'(calendar\.google\.com|discord\.(gg|com)|senja\.io|zoom\.us|veed\.io|drive\.google\.com|arxiv\.org)',
    re.IGNORECASE
)

_MIRO_PATTERN  = re.compile(r'miro\.com', re.IGNORECASE)
_COLAB_PATTERN = re.compile(r'colab\.research\.google\.com|colab\.google', re.IGNORECASE)
_ZOOM_PATTERN  = re.compile(r'zoom\.us', re.IGNORECASE)


def _classify_item(item: dict) -> str:
    """
    Classify a lesson item as 'miro', 'colab', or 'video'.
    Checks embedUrl first, then description, then title — never trusts the type field.
    """
    for text in (
        item.get("embedUrl") or "",
        item.get("description") or "",
        item.get("title") or "",
    ):
        if _MIRO_PATTERN.search(text):
            return "miro"
        if _COLAB_PATTERN.search(text):
            return "colab"
    return "video"


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
    result = {"videoUrl": None, "embedUrl": None, "miroBoardUrls": [], "colabUrls": []}

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

    for miro_id in group.get("miroIds", []):
        doc = prod_db.collection("Lessons").document(miro_id).get()
        if doc.exists:
            data = doc.to_dict()
            # Miro URL — check embedUrl first, then description, never trust type.
            url = next(
                (
                    candidate
                    for candidate in (
                        data.get("embedUrl") or "",
                        next(
                            (
                                m.group().rstrip(".,;)")
                                for m in re.finditer(r'https?://\S+', data.get("description") or "")
                                if _MIRO_PATTERN.search(m.group())
                            ),
                            "",
                        ),
                    )
                    if _MIRO_PATTERN.search(candidate)
                ),
                None,
            )
            if url:
                result["miroBoardUrls"].append(url)

    for colab_id in group.get("colabIds", []):
        doc = prod_db.collection("Lessons").document(colab_id).get()
        if doc.exists:
            data = doc.to_dict()
            # Colab URL lives in the lesson description — extract it by pattern.
            description = data.get("description") or ""
            url = next(
                (
                    m.group().rstrip(".,;)")
                    for m in re.finditer(r'https?://\S+', description)
                    if _COLAB_PATTERN.search(m.group())
                ),
                None,
            )
            if url:
                result["colabUrls"].append(url)

    # Final safety net — ensure no ignored URL slipped through as a video source
    result["miroBoardUrls"] = [u for u in result["miroBoardUrls"] if not _IGNORED_URL_PATTERNS.search(u)]
    if result["videoUrl"] and _IGNORED_URL_PATTERNS.search(result["videoUrl"]):
        result["videoUrl"] = None
        result["embedUrl"] = None

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
    if _is_direct_file(url):
        output_path = os.path.join(output_dir, "audio.mp3")
        import urllib.request
        urllib.request.urlretrieve(url, output_path)
        return output_path

    # Download best audio stream without ffmpeg postprocessing.
    # Whisper accepts webm/m4a/mp4/mp3 so no conversion is needed.
    cookies_file = Path(__file__).parent / "yt-cookies.txt"
    cmd = [YT_DLP, "--format", "bestaudio", "--output", "audio.%(ext)s",
           "--no-playlist", "--fixup", "never", "--remote-components", "ejs:github"]
    if cookies_file.exists():
        cmd += ["--cookies", str(cookies_file)]
    cmd.append(url)
    # Ensure deno is on PATH for the n-challenge solver
    env = {**os.environ, "PATH": os.environ.get("PATH", "") + ":/home/teamvizuara/.deno/bin"}
    result = subprocess.run(cmd, capture_output=True, text=True, cwd=output_dir, env=env)
    if result.returncode != 0:
        raise RuntimeError(f"yt-dlp failed: {result.stderr[:1000]}")

    # Find whichever audio file was written (e.g. audio.webm, audio.m4a)
    for f in Path(output_dir).iterdir():
        if f.stem == "audio":
            return str(f)
    raise RuntimeError("yt-dlp succeeded but no audio file found in output dir")


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
         "-c", "copy", str(chunk_dir / "chunk_%03d.mp4")],
        check=True, capture_output=True
    )
    all_segments, offset = [], 0.0
    for chunk in sorted(chunk_dir.glob("chunk_*.mp4")):
        result = _transcribe_single(str(chunk))
        for seg in result["segments"]:
            all_segments.append({**seg, "start": seg["start"] + offset, "end": seg["end"] + offset})
        if result["segments"]:
            offset = all_segments[-1]["end"]
    return {"segments": all_segments, "fullText": " ".join(s["text"] for s in all_segments)}


# ══════════════════════════════════════════════════════════════════════════════
# STEP 2: TRANSCRIPT AUDIT
# ══════════════════════════════════════════════════════════════════════════════

AUDIT_SYSTEM = """
You are a transcript editor for educational video content on machine learning and AI.
Correct errors in the auto-generated Whisper transcript below.

Fix:
- Misheared technical terms, model names, paper titles, author names
- Mathematical notation (e.g. "eigen values" → "eigenvalues", "relu" → "ReLU")
- Incorrect word boundaries and obvious punctuation errors

Do NOT:
- Change the meaning or substance of what was said
- Remove or add content
- Rephrase or improve the prose — only fix clear errors

Return ONLY the corrected transcript text, nothing else.
"""


def audit_transcript(transcript: dict, lesson_title: str) -> dict:
    """LLM-correct Whisper errors in the transcript fullText."""
    corrected = _llm_raw(
        AUDIT_SYSTEM,
        f"Lesson title: {lesson_title}\n\nTranscript:\n{transcript['fullText'][:MAX_AUDIT_CHARS]}",
        max_tokens=16384,
    )
    return {**transcript, "fullText": corrected.strip()}


# ══════════════════════════════════════════════════════════════════════════════
# STEP 3: LLM EXTRACTION
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
# STEP 4: SUMMARY GENERATION
# ══════════════════════════════════════════════════════════════════════════════

SUMMARY_LATEX_SYSTEM = """
You are an expert technical writer. Write a comprehensive long-form lesson summary
in LaTeX from the lecture transcript and metadata provided.

LaTeX body rules:
- No preamble. No \\documentclass, \\usepackage, \\begin{document}, or \\end{document}.
- Use \\section{} and \\subsection{} based on the chapter markers provided.
- Use proper LaTeX math: inline $...$, display \\[...\\], \\begin{equation}...\\end{equation}
- Escape special characters outside math: & → \\&  % → \\%  # → \\#  _ → \\_  ~ → \\textasciitilde{}
- Only use packages: graphicx, amsmath, amssymb, hyperref, booktabs, enumitem, float
- Cover all key concepts with clear prose. Define new terms on first use.
- At appropriate points insert on its own line:
  [FIGURE: "precise, visual description for an image generation API"]
  (2-6 figures total)
- STRICT: Do NOT use any Markdown syntax. No ##, **, *, __, >, -, ```, or other Markdown
  constructs. Use only LaTeX commands for all formatting and structure.

Return ONLY the raw LaTeX body — no JSON, no markdown fences, no preamble.
"""

SUMMARY_MARKDOWN_SYSTEM = """
You are an expert technical writer. Convert the provided LaTeX lesson summary into
an equivalent Markdown version covering identical content.

Markdown body rules:
- Use ## and ### headings mirroring the \\section{} and \\subsection{} structure in the LaTeX.
- Use standard Markdown math: inline $...$ and display $$...$$
- Use standard Markdown formatting (bold, italic, tables, lists, code blocks)
- Cover all the same content and concepts as the LaTeX version.
- Preserve every [FIGURE: "..."] placeholder at the exact equivalent point, with identical text.
- STRICT: Do NOT use any LaTeX commands outside of math delimiters. No \\section{}, \\begin{},
  \\end{}, \\textbf{}, \\emph{}, \\item, \\label{}, or any other LaTeX commands. Convert all
  LaTeX formatting to its Markdown equivalent (e.g. \\textbf{x} → **x**, \\emph{x} → *x*).
  Math inside $...$ or $$...$$ is the only permitted use of backslash-commands.

Return ONLY the raw Markdown body — no JSON, no markdown fences.
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
    latex = _strip_fences(_llm_raw(SUMMARY_LATEX_SYSTEM, user, max_tokens=16384))
    markdown = _strip_fences(_llm_raw(SUMMARY_MARKDOWN_SYSTEM, latex, max_tokens=16384))
    return {
        "contentLatex":    latex,
        "contentMarkdown": markdown,
        "figureCount":     latex.count("[FIGURE:"),
    }


def _fmt_ts(seconds: float) -> str:
    m, s = divmod(int(seconds), 60)
    h, m = divmod(m, 60)
    return f"{h:02d}:{m:02d}:{s:02d}" if h else f"{m:02d}:{s:02d}"


# ══════════════════════════════════════════════════════════════════════════════
# STEP 5: FIGURE GENERATION (PaperBanana)
# ══════════════════════════════════════════════════════════════════════════════

FIGURE_BRIEF_SYSTEM = """
You are a technical diagram director for an educational platform teaching machine learning and AI.

You will be given a short placeholder description of a diagram and the surrounding lesson text
where the diagram will appear.

Write a precise, detailed diagram brief for an AI image generator. Specify:
- What entities, steps, or components should appear
- How they relate or connect (flow, hierarchy, comparison, etc.)
- What labels or annotations are essential for student understanding
- What concept the diagram must make visually obvious

Rules:
- Be specific and visual — describe what should literally appear on the diagram
- Do not describe colours, style, or aesthetics (handled separately)
- 3-6 sentences maximum
- Return ONLY the diagram brief, nothing else
"""


def _enrich_figure_description(raw_description: str, surrounding_context: str) -> str:
    """Use an LLM to expand a one-line figure placeholder into a detailed diagram brief."""
    user = (
        f"Placeholder description: {raw_description}\n\n"
        f"Surrounding lesson text:\n{surrounding_context}"
    )
    return _llm_raw(FIGURE_BRIEF_SYSTEM, user, max_tokens=512).strip()


def generate_figures(course_id: str, lesson_id: str, summary: dict) -> dict:
    # Process the Markdown version — figures become ![alt](url) image tags.
    # Collect description→url so the same URLs can be embedded in the LaTeX version.
    content = summary["contentMarkdown"]
    latex   = summary["contentLatex"]
    desc_to_url = {}   # description → resolved URL
    index = [0]

    def replace(match):
        description = match.group(1)
        if description in desc_to_url:
            return f"![{description[:60]}]({desc_to_url[description]})"
        # Extract surrounding text to give the enrichment LLM context
        ctx_start = max(0, match.start() - 600)
        ctx_end   = min(len(content), match.end() + 600)
        surrounding = content[ctx_start:match.start()] + content[match.end():ctx_end]
        enriched = _enrich_figure_description(description, surrounding)
        try:
            url = _paperbanana_and_upload(enriched, course_id, lesson_id, index[0])
            desc_to_url[description] = url
            index[0] += 1
            return f"![{description[:60]}]({url})"
        except Exception as e:
            print(f"  [WARN] Figure {index[0]} failed: {e}")
            index[0] += 1
            return f"<!-- figure failed: {description[:80]} -->"

    final_md = FIGURE_PATTERN.sub(replace, content)

    def replace_latex(match):
        description = match.group(1)
        url = desc_to_url.get(description)
        if not url:
            return f"% figure failed: {description[:80]}"
        caption = description[:120].replace("{", r"\{").replace("}", r"\}")
        return (
            f"\\begin{{figure}}[H]\n"
            f"\\centering\n"
            f"\\includegraphics[width=\\linewidth]{{{url}}}\n"
            f"\\caption{{{caption}}}\n"
            f"\\end{{figure}}"
        )

    final_latex = FIGURE_PATTERN.sub(replace_latex, latex)

    return {
        "content":            final_md,     # Markdown with resolved images
        "contentRaw":         final_latex,  # LaTeX with resolved \begin{figure}[H] blocks
        "contentMarkdownRaw": content,      # Markdown with [FIGURE:] placeholders
        "figureCount":        len(desc_to_url),
    }


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
    questions = _llm_json_array(QUIZ_SYSTEM, user, max_tokens=8192)

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
        "content":            summary.get("content", ""),             # Markdown with resolved images
        "contentRaw":         summary.get("contentRaw", ""),          # LaTeX with [FIGURE:] placeholders
        "contentMarkdownRaw": summary.get("contentMarkdownRaw", ""),  # Markdown with [FIGURE:] placeholders
        "figureCount":        summary.get("figureCount", 0),
        "createdAt": SERVER_TIMESTAMP,
    })


def write_lesson_to_orbit(course_id: str, topic_id: str, group: dict, urls: dict, video_lesson: dict, extraction: dict):
    """
    Write the merged Orbit lesson document to courses/{courseId}/lessons/{lessonId}.
    `topic_id`    — parent OrbitTopic id
    `group`       — LLM-produced group { title, videoId, miroIds, colabIds }
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
        "miroBoardUrls": urls["miroBoardUrls"],  # iframe-embeddable Miro URLs
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


def write_miro_lesson_to_orbit(course_id: str, topic_id: str, group: dict, urls: dict):
    """
    Write a standalone Miro lesson (no video, no summary) to Lessons/{lessonId}.
    Lesson ID = first miroId.
    """
    lesson_id = group["miroIds"][0]
    orbit_db.collection("Lessons").document(lesson_id).set({
        "id": lesson_id,
        "courseId": course_id,
        "topicId": topic_id,
        "title": group.get("title", ""),
        "type": "MIRO NOTES",
        "description": "",
        "miroBoardUrls": urls["miroBoardUrls"],
        "durationAddedToLearningProgress": False,
        "createdAt": SERVER_TIMESTAMP,
        "updatedAt": SERVER_TIMESTAMP,
    })


def write_zoom_lesson_to_orbit(course_id: str, topic_id: str, group: dict, prod_lesson: dict):
    """
    Copy a Zoom lesson as-is from the production DB to Orbit.
    No transcription, extraction, or summary — the lesson is preserved verbatim.
    Lesson ID = videoId.
    """
    lesson_id = group["videoId"]
    orbit_db.collection("Lessons").document(lesson_id).set({
        "id": lesson_id,
        "courseId": course_id,
        "topicId": topic_id,
        "title": group.get("title") or prod_lesson.get("title", ""),
        "type": "ZOOM",
        "description": prod_lesson.get("description", ""),
        "embedUrl": prod_lesson.get("embedUrl", ""),
        "duration": prod_lesson.get("duration"),
        "durationAddedToLearningProgress": False,
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
    Process one lesson group (video + optional miros + optional colabs, or standalone miros).
    - STANDARD lesson: videoId is the Orbit lesson ID; runs all 5 pipeline steps.
    - MIRO NOTES lesson: no video; first miroId is the lesson ID; written directly, no pipeline steps.
    Returns the lesson_id string on success, None if skipped.
    """
    video_id  = group.get("videoId")
    miro_ids  = group.get("miroIds", [])

    # ── Standalone Miro lesson ────────────────────────────────────────────────
    if not video_id:
        if not miro_ids:
            print(f"  → Skipping group '{group.get('title', '')}' — no video or Miro")
            return None
        lesson_id  = miro_ids[0]
        miro_count = len(miro_ids)
        print(f"  → Miro lesson: {group.get('title', lesson_id)}"
              + f" [{miro_count} board{'s' if miro_count > 1 else ''}]")
        urls = resolve_group_urls(group)
        write_miro_lesson_to_orbit(course_id, topic_id, group, urls)
        set_lesson_state(course_id, lesson_id, {"status": "done"})
        print(f"    ✓ Miro lesson complete")
        return lesson_id

    # ── Standard lesson (video) ───────────────────────────────────────────────
    lesson_id = video_id
    state = get_lesson_state(course_id, lesson_id)
    done = set(state.get("stepsCompleted", []))

    miro_count  = len(miro_ids)
    colab_count = len(group.get("colabIds", []))
    print(f"  → Lesson: {group.get('title', lesson_id)}"
          + (f" [+{miro_count} Miro]" if miro_count else "")
          + (f" [+{colab_count} Colab]" if colab_count else ""))

    # ── Zoom lesson check (before URL resolution) ─────────────────────────────
    prod_lesson_raw = fetch_lesson(lesson_id)
    _zoom_candidate = next(
        (
            text for text in (
                prod_lesson_raw.get("embedUrl") or "",
                prod_lesson_raw.get("description") or "",
            )
            if _ZOOM_PATTERN.search(text)
        ),
        None,
    )
    if _zoom_candidate:
        print(f"    → Zoom lesson detected — copying as-is")
        write_zoom_lesson_to_orbit(course_id, topic_id, group, prod_lesson_raw)
        set_lesson_state(course_id, lesson_id, {"status": "done"})
        print(f"    ✓ Zoom lesson complete")
        return lesson_id

    # Resolve all URLs from the production DB lesson documents
    urls = resolve_group_urls(group)
    if not urls["videoUrl"]:
        if miro_ids:
            miro_lesson_id = miro_ids[0]
            print(f"  [WARN] Could not resolve video URL for lesson {lesson_id}, writing as Miro-only")
            write_miro_lesson_to_orbit(course_id, topic_id, group, urls)
            set_lesson_state(course_id, miro_lesson_id, {"status": "done"})
            set_lesson_state(course_id, lesson_id, {"status": "done", "resolvedLessonId": miro_lesson_id})
            print(f"    ✓ Miro-only lesson complete")
            return miro_lesson_id
        print(f"  [WARN] Could not resolve video URL for lesson {lesson_id}, skipping")
        return None

    # Use the already-fetched prod lesson doc for metadata fields
    video_lesson = prod_lesson_raw

    # ── Step 1: Transcription ────────────────────────────────────────────────
    if "transcription" not in done:
        print("    [1/5] Transcribing...")
        set_lesson_state(course_id, lesson_id, {"status": "transcribing"})
        transcript = transcribe(urls["videoUrl"])
        write_transcript(course_id, lesson_id, transcript)
        mark_step_done(course_id, lesson_id, "transcription")
    else:
        print("    [1/5] Transcription already done, loading...")
        transcript = orbit_db.collection("Transcripts").document(f"{course_id}_{lesson_id}").get().to_dict()

    # ── Step 2: Transcript Audit ─────────────────────────────────────────────
    if "audit" not in done:
        print("    [2/5] Auditing transcript...")
        set_lesson_state(course_id, lesson_id, {"status": "auditing"})
        lesson_title = group.get("title") or video_lesson.get("title", "")
        raw_text = transcript["fullText"]
        transcript = audit_transcript(transcript, lesson_title)
        orbit_db.collection("Transcripts").document(f"{course_id}_{lesson_id}").update({
            "rawFullText": raw_text,
            "fullText": transcript["fullText"],
            "audited": True,
        })
        mark_step_done(course_id, lesson_id, "audit")
    else:
        print("    [2/5] Audit already done.")

    # ── Step 3: LLM Extraction ───────────────────────────────────────────────
    if "extraction" not in done:
        print("    [3/5] Extracting metadata...")
        set_lesson_state(course_id, lesson_id, {"status": "extracting"})
        lesson_meta = {**video_lesson, "title": group.get("title") or video_lesson.get("title", "")}
        extraction = llm_extract(transcript, lesson_meta)
        # Write the lesson document — this is the single source of truth for all metadata
        write_lesson_to_orbit(course_id, topic_id, group, urls, video_lesson, extraction)
        mark_step_done(course_id, lesson_id, "extraction")
    else:
        print("    [3/5] Extraction already done, loading...")
        extraction = orbit_db.collection("Lessons").document(lesson_id).get().to_dict()

    # ── Step 4: Summary Generation ───────────────────────────────────────────
    if "summary" not in done:
        print("    [4/5] Generating summary...")
        set_lesson_state(course_id, lesson_id, {"status": "summarizing"})
        lesson_title = group.get("title") or video_lesson.get("title", "")
        summary = generate_summary(transcript, extraction, lesson_title)
        mark_step_done(course_id, lesson_id, "summary")
    else:
        print("    [4/5] Summary already done, loading...")
        saved = orbit_db.collection("LessonSummaries").document(f"{course_id}_{lesson_id}").get().to_dict() or {}
        summary = {
            "contentLatex":    saved.get("contentRaw", ""),
            "contentMarkdown": saved.get("contentMarkdownRaw", ""),
            "figureCount":     saved.get("figureCount", 0),
        }

    # ── Step 5: Figure Generation ────────────────────────────────────────────
    if "figures" not in done:
        print(f"    [5/5] Generating {summary['figureCount']} figures...")
        set_lesson_state(course_id, lesson_id, {"status": "figures"})
        final_summary = generate_figures(course_id, lesson_id, summary)
        write_summary(course_id, lesson_id, final_summary)
        mark_step_done(course_id, lesson_id, "figures")
    else:
        print("    [5/5] Figures already done.")

    set_lesson_state(course_id, lesson_id, {"status": "done"})
    print(f"    ✓ Lesson complete")
    return lesson_id


_PREVIEW_URL_PATTERN = re.compile(r'(youtube\.com|youtu\.be|vimeo\.com)', re.IGNORECASE)


def _get_first_lesson_video_url(orbit_topics: list) -> str | None:
    """Return the videoUrl of the first non-quiz lesson with a YouTube or Vimeo URL."""
    for topic in orbit_topics:
        for lesson_id in topic.get("lessonIds", []):
            if lesson_id.startswith("quiz_"):
                continue
            doc = orbit_db.collection("Lessons").document(lesson_id).get()
            if doc.exists:
                url = doc.to_dict().get("videoUrl")
                if url and _PREVIEW_URL_PATTERN.search(url):
                    return url
    return None


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
            # Primary ID: video if present, otherwise first Miro board
            primary_id = group.get("videoId") or next(iter(group.get("miroIds", [])), None)
            if not primary_id:
                print(f"    → Group {i+1}/{len(groups)}: '{group.get('title','')}' — no video or Miro, skipping")
                continue

            lesson_state = get_lesson_state(course_id, primary_id)
            if lesson_state.get("status") == "done":
                print(f"    → Lesson {i+1}/{len(groups)} already done, skipping")
                topic_lesson_ids.append(lesson_state.get("resolvedLessonId", primary_id))
                continue

            try:
                result = process_lesson(course_id, topic_id, group)
                if result is not None:
                    topic_lesson_ids.append(result)
            except Exception as e:
                print(f"    [ERROR] Lesson {primary_id} failed: {e}")
                traceback.print_exc()
                set_lesson_state(course_id, primary_id, {"status": "failed", "error": str(e)})
                set_course_state(course_id, {"status": "failed", "failedAt": primary_id})
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
                traceback.print_exc()
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
    preview_url = _get_first_lesson_video_url(orbit_topics)
    if preview_url:
        orbit_fields["previewUrl"] = preview_url
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


def _strip_fences(text: str) -> str:
    """Remove markdown code fences (```json ... ``` or ``` ... ```)."""
    return re.sub(r'^```[a-z]*\n?', '', text.strip(), flags=re.IGNORECASE).rstrip('`').strip()


def _fix_json_escapes(text: str) -> str:
    """Fix invalid JSON escape sequences (e.g. LaTeX backslashes like \frac, \alpha)."""
    return re.sub(r'\\(?!["\\/bfnrtu])', r'\\\\', text)


def _fix_literal_newlines(text: str) -> str:
    """Escape literal newlines inside JSON string values."""
    result = []
    in_string = False
    escaped = False
    for ch in text:
        if escaped:
            result.append(ch)
            escaped = False
        elif ch == '\\':
            result.append(ch)
            escaped = True
        elif ch == '"':
            result.append(ch)
            in_string = not in_string
        elif ch == '\n' and in_string:
            result.append('\\n')
        else:
            result.append(ch)
    return ''.join(result)


def _parse_json_object(text: str) -> dict:
    candidates = [text, _strip_fences(text)]
    all_candidates = []
    for c in candidates:
        all_candidates.append(c)
        all_candidates.append(_fix_literal_newlines(c))
        all_candidates.append(_fix_json_escapes(c))
        all_candidates.append(_fix_json_escapes(_fix_literal_newlines(c)))
    for candidate in all_candidates:
        try:
            return json.loads(candidate)
        except json.JSONDecodeError:
            match = re.search(r'\{.*\}', candidate, re.DOTALL)
            if match:
                try:
                    return json.loads(match.group())
                except json.JSONDecodeError:
                    pass
    raise ValueError(f"No JSON object found in LLM response: {text[:200]}")


def _parse_json_array(text: str) -> list:
    candidates = [text, _strip_fences(text)]
    all_candidates = []
    for c in candidates:
        all_candidates.append(c)
        all_candidates.append(_fix_literal_newlines(c))
        all_candidates.append(_fix_json_escapes(c))
        all_candidates.append(_fix_json_escapes(_fix_literal_newlines(c)))
    for candidate in all_candidates:
        try:
            return json.loads(candidate)
        except json.JSONDecodeError:
            match = re.search(r'\[.*\]', candidate, re.DOTALL)
            if match:
                try:
                    return json.loads(match.group())
                except json.JSONDecodeError:
                    pass
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
