#!/usr/bin/env python3
"""
Vizuara Orbit — Standalone Content Pipeline

Processes prod Firestore lessons that contain YouTube or Vimeo URLs.
Runs transcription → audit → extraction → summary+figures → quiz for each
valid lesson and fills Transcripts, LessonSummaries, and Quizzes collections
in the production Firestore.

Fully resumable: each step is gated by _PipelineState, so a crashed run
picks up exactly where it left off.

Usage:
    python run_content_pipeline.py [course_id ...]

    If no course IDs are passed, runs all IDs in COURSE_IDS below.
"""

import os
import re
import sys
import uuid
import random
import subprocess
import tempfile
import traceback
from pathlib import Path

import download_zoom

from google.cloud.firestore_v1 import SERVER_TIMESTAMP

# Import stateless processing functions and Firebase clients from run_pipeline.
# Importing it triggers Firebase initialisation — both service account files must exist.
import run_pipeline
from run_pipeline import (
    prod_db,
    orbit_db,
    prod_bucket,
    embed_to_video_url,
    _extract_url_from_description,
    _IGNORED_URL_PATTERNS,
    _PREVIEW_URL_PATTERN,
    _llm_raw,
    _llm_json_array,
    QUIZ_SYSTEM,
    MAX_CHARS_PER_LESSON,
    FIGURE_STYLE_CONTEXT,
    transcribe,
    _transcribe_audio,
    audit_transcript,
    llm_extract,
    generate_summary,
    generate_figures,
)

# ── Google API key rotation ───────────────────────────────────────────────────
# Supports a comma-separated GOOGLE_API_KEY and/or GOOGLE_API_KEY_2 ... _9.
_raw_key = os.getenv("GOOGLE_API_KEY", "")
_GOOGLE_API_KEYS: list[str] = [k.strip() for k in _raw_key.split(",") if k.strip()]
for _i in range(2, 10):
    _extra = os.getenv(f"GOOGLE_API_KEY_{_i}", "")
    if _extra.strip():
        _GOOGLE_API_KEYS.append(_extra.strip())


def _is_quota_error(exc: Exception) -> bool:
    msg = str(exc).lower()
    return any(kw in msg for kw in ("quota", "resource_exhausted", "429", "rate limit", "ratelimit"))


def _paperbanana_and_upload_with_fallback(description: str, course_id: str, lesson_id: str, index: int) -> str:
    """
    Identical to run_pipeline._paperbanana_and_upload but rotates through
    _GOOGLE_API_KEYS when a quota / rate-limit error is encountered.
    """
    import asyncio
    from paperbanana import DiagramType, GenerationInput, PaperBananaPipeline
    from paperbanana.core.config import Settings

    if not _GOOGLE_API_KEYS:
        raise RuntimeError("No GOOGLE_API_KEY configured")

    last_exc: Exception | None = None
    for key in _GOOGLE_API_KEYS:
        os.environ["GOOGLE_API_KEY"] = key
        try:
            settings = Settings(
                vlm_provider="gemini",
                vlm_model="gemini-2.5-flash",
                image_provider="google_imagen",
                image_model="gemini-3-pro-image-preview",
                refinement_iterations=3,
            )
            pipeline = PaperBananaPipeline(settings=settings)
            async def _run():
                return await asyncio.wait_for(
                    pipeline.generate(
                        GenerationInput(
                            source_context=FIGURE_STYLE_CONTEXT + "\n\nDiagram to generate:\n" + description,
                            communicative_intent=description,
                            diagram_type=DiagramType.METHODOLOGY,
                        )
                    ),
                    timeout=360,
                )
            result = asyncio.run(_run())
            storage_path = f"lesson_figures/{course_id}/{lesson_id}/figure_{index:03d}.png"
            blob = run_pipeline.orbit_bucket.blob(storage_path)
            blob.upload_from_filename(result.image_path, content_type="image/png")
            blob.make_public()
            return blob.public_url
        except Exception as exc:
            if _is_quota_error(exc):
                print(f"  [WARN] Google API key quota exceeded, trying next key...")
                last_exc = exc
                continue
            raise

    raise RuntimeError(f"All Google API keys exhausted") from last_exc


# Patch into run_pipeline so generate_figures() picks up the fallback version.
run_pipeline._paperbanana_and_upload = _paperbanana_and_upload_with_fallback


def _duration_from_transcript(transcript: dict) -> dict:
    """Derive {hours, minutes} from the last segment's end timestamp (seconds)."""
    segments = transcript.get("segments", [])
    total_seconds = segments[-1]["end"] if segments else 0
    return {"hours": int(total_seconds // 3600), "minutes": int((total_seconds % 3600) // 60)}


CATEGORY_SYSTEM = """
You are a curriculum cataloguer. Given a course title and excerpts from its lesson
transcripts, assign up to 5 category names from the provided list of existing categories.
If none of the existing categories are a good fit, you may propose new ones.

Return ONLY a JSON array of strings (category names), max 5 items:
["Category One", "Category Two"]
"""

TAGS_SYSTEM = """
You are a curriculum cataloguer. Given a course title and excerpts from its lesson
transcripts, generate up to 3 short tags for the course.

Tags must be different from broad category names — they should be specific, descriptive
keywords that reflect the course's precise subject matter, techniques, or tools
(e.g. "backpropagation", "PyTorch", "time series"). Avoid generic terms like
"machine learning" or "data science" that belong at the category level.

Return ONLY a JSON array of strings, max 3 items:
["tag one", "tag two", "tag three"]
"""


def _fetch_category_names() -> list[str]:
    """Return all existing CATEGORY Attribute names from the Orbit Attributes collection."""
    docs = orbit_db.collection("Attributes").where("type", "==", "CATEGORY").get()
    return [d.to_dict().get("name", "") for d in docs if d.to_dict().get("name")]

# ─── Course IDs to process ────────────────────────────────────────────────────
COURSE_IDS = [
    # "course_id_01",
]


# ══════════════════════════════════════════════════════════════════════════════
# PROD FIRESTORE WRITERS
# (parallel to the orbit_db writers in run_pipeline.py, but targeting prod_db)
# ══════════════════════════════════════════════════════════════════════════════

def get_course_state(course_id: str) -> dict:
    doc = prod_db.collection("_PipelineStateProd").document(course_id).get()
    return doc.to_dict() or {} if doc.exists else {}


def set_course_state(course_id: str, data: dict):
    prod_db.collection("_PipelineStateProd").document(course_id).set(
        {**data, "updatedAt": SERVER_TIMESTAMP}, merge=True
    )


def get_lesson_state(course_id: str, lesson_id: str) -> dict:
    doc = (
        prod_db.collection("_PipelineStateProd")
        .document(course_id)
        .collection("Lessons")
        .document(lesson_id)
        .get()
    )
    return doc.to_dict() or {} if doc.exists else {}


def set_lesson_state(course_id: str, lesson_id: str, data: dict):
    (
        prod_db.collection("_PipelineStateProd")
        .document(course_id)
        .collection("Lessons")
        .document(lesson_id)
        .set({**data, "updatedAt": SERVER_TIMESTAMP}, merge=True)
    )


def _mark_step_done(course_id: str, lesson_id: str, step: str):
    state = get_lesson_state(course_id, lesson_id)
    done = list(set(state.get("stepsCompleted", []) + [step]))
    set_lesson_state(course_id, lesson_id, {"stepsCompleted": done})


def write_transcript(course_id: str, lesson_id: str, transcript: dict):
    prod_db.collection("Transcripts").document(f"{course_id}_{lesson_id}").set({
        "courseId": course_id,
        "lessonId": lesson_id,
        **transcript,
        "createdAt": SERVER_TIMESTAMP,
    })


def write_summary(course_id: str, lesson_id: str, summary: dict):
    prod_db.collection("LessonSummaries").document(f"{course_id}_{lesson_id}").set({
        "courseId": course_id,
        "lessonId": lesson_id,
        "content":            summary.get("content", ""),
        "contentRaw":         summary.get("contentRaw", ""),
        "contentMarkdownRaw": summary.get("contentMarkdownRaw", ""),
        "figureCount":        summary.get("figureCount", 0),
        "createdAt": SERVER_TIMESTAMP,
    })


def write_quiz(course_id: str, topic_id: str, quiz: dict):
    prod_db.collection("MandatoryQuizzes").document(topic_id).set({
        "id": topic_id,
        "courseId": course_id,
        "topicId": topic_id,
        "sourceLessonIds": quiz["sourceLessonIds"],
        "questions": quiz["questions"],
        "createdAt": SERVER_TIMESTAMP,
    })


def generate_quiz(course_id: str, topic_id: str, lesson_ids: list) -> dict:
    """Generate a mandatory quiz for a topic, reading from prod Firestore."""
    parts = []
    for i, lid in enumerate(lesson_ids):
        t = prod_db.collection("Transcripts").document(f"{course_id}_{lid}").get().to_dict()
        l = prod_db.collection("Lessons").document(lid).get().to_dict()
        full_text = (t or {}).get("fullText", "")[:MAX_CHARS_PER_LESSON]
        concepts  = ", ".join((l or {}).get("keyConcepts", []))
        title     = (l or {}).get("title", lid)
        parts.append(
            f"--- Lesson {i+1}: {title} (ID: {lid}) ---\n"
            f"Key concepts: {concepts}\n\nTranscript:\n{full_text}"
        )
    user = f"Topic ID: {topic_id} — {len(lesson_ids)} lesson(s)\n\n" + "\n\n".join(parts)
    questions = _llm_json_array(QUIZ_SYSTEM, user, max_tokens=8192)
    for q in questions:
        if not q.get("id"):
            q["id"] = f"q_{uuid.uuid4().hex[:8]}"
    return {"topicId": topic_id, "sourceLessonIds": lesson_ids, "questions": questions}


def generate_course_categories(course_id: str, course_title: str, topics: dict[str, list[str]]) -> list[str]:
    """Derive up to 5 category names for a course from its prod lesson transcripts."""
    existing = _fetch_category_names()
    all_lesson_ids = [lid for ids in topics.values() for lid in ids]

    transcript_excerpts = []
    for lid in all_lesson_ids:
        t = prod_db.collection("Transcripts").document(f"{course_id}_{lid}").get()
        if t.exists:
            text = (t.to_dict() or {}).get("fullText", "")[:3000]
            if text:
                transcript_excerpts.append(text)

    existing_str = (
        f"Existing categories (prefer these): {', '.join(existing)}\n\n"
        if existing else ""
    )
    user = (
        f"Course title: {course_title}\n\n"
        + existing_str
        + "Transcript excerpts:\n"
        + "\n---\n".join(transcript_excerpts[:10])
    )
    return _llm_json_array(CATEGORY_SYSTEM, user, max_tokens=256)[:5]


LESSON_DESCRIPTION_SYSTEM = """
You are a curriculum designer. Given a lesson title and its transcript, write a
5-6 sentence description of the lesson — what it covers, what students will learn,
and why it matters.

Return ONLY the description as plain text, no JSON, no markdown.
"""


def generate_lesson_description(transcript: dict, lesson_title: str) -> str:
    """Generate a 5-6 sentence description for a lesson from its transcript."""
    full_text = transcript.get("fullText", "")[:12000]
    user = f"Lesson title: {lesson_title}\n\nTranscript:\n{full_text}"
    return _llm_raw(LESSON_DESCRIPTION_SYSTEM, user, max_tokens=512).strip()


DESCRIPTION_SYSTEM = """
You are a curriculum designer. Given a course title and the short descriptions of its
lessons (skip any that are empty), write a single cohesive paragraph that describes
the course — what it covers, what students will learn, and who it is for.

Return ONLY the paragraph as plain text, no JSON, no markdown.
"""


def generate_course_description(course_id: str, course_title: str, all_lesson_ids: list[str]) -> str:
    """Build a course description from lesson shortDescriptions, falling back to description."""
    parts = []
    for lid in all_lesson_ids:
        d = prod_db.collection("Lessons").document(lid).get().to_dict() or {}
        text = (d.get("shortDescription") or d.get("description") or "").strip()
        title = (d.get("title") or lid).strip()
        if text:
            parts.append(f"- {title}: {text}")

    if not parts:
        return ""

    user = f"Course title: {course_title}\n\nLesson descriptions:\n" + "\n".join(parts)
    return _llm_raw(DESCRIPTION_SYSTEM, user, max_tokens=512).strip()


SLUG_DISAMBIGUATE_SYSTEM = """
You are given a course title and a URL slug that is already taken.
Suggest a new slug by appending 1-3 descriptive words that naturally extend the title.
Return ONLY the new slug as a single lowercase hyphenated string, nothing else.
"""


def _title_to_slug(title: str) -> str:
    slug = title.lower().strip()
    slug = re.sub(r"[^a-z0-9\s-]", "", slug)
    slug = re.sub(r"[\s]+", "-", slug)
    slug = re.sub(r"-+", "-", slug).strip("-")
    return slug


def _slug_exists(slug: str) -> bool:
    docs = prod_db.collection("Courses").where("slug", "==", slug).limit(1).get()
    return len(docs) > 0


def generate_course_slug(course_title: str) -> str:
    """
    Derive a unique slug from the course title.
    If the base slug is taken, ask the LLM to extend it, then verify uniqueness.
    """
    slug = _title_to_slug(course_title)
    if not _slug_exists(slug):
        return slug

    # Slug is taken — let LLM extend it until unique (max 5 attempts)
    current = slug
    for _ in range(5):
        user = f"Course title: {course_title}\nTaken slug: {current}"
        current = _title_to_slug(_llm_raw(SLUG_DISAMBIGUATE_SYSTEM, user, max_tokens=32))
        if current and not _slug_exists(current):
            return current

    # Fallback: append course_id fragment (should never be needed in practice)
    return slug


def generate_course_tags(course_id: str, course_title: str, topics: dict[str, list[str]]) -> list[str]:
    """Derive up to 3 specific tags for a course from its prod lesson transcripts."""
    all_lesson_ids = [lid for ids in topics.values() for lid in ids]

    transcript_excerpts = []
    for lid in all_lesson_ids:
        t = prod_db.collection("Transcripts").document(f"{course_id}_{lid}").get()
        if t.exists:
            text = (t.to_dict() or {}).get("fullText", "")[:3000]
            if text:
                transcript_excerpts.append(text)

    user = (
        f"Course title: {course_title}\n\n"
        + "Transcript excerpts:\n"
        + "\n---\n".join(transcript_excerpts[:10])
    )
    return _llm_json_array(TAGS_SYSTEM, user, max_tokens=128)[:3]


# ══════════════════════════════════════════════════════════════════════════════
# HELPERS
# ══════════════════════════════════════════════════════════════════════════════

def _get_lesson_url(lesson: dict) -> str:
    """Return the first URL found in embedUrl, then description. No type filtering."""
    embed = lesson.get("embedUrl") or ""
    if embed:
        return embed
    # Scan raw — _extract_url_from_description filters out Zoom/calendar links which
    # we still need for duration classification.
    for match in re.finditer(r'https?://\S+', lesson.get("description") or ""):
        url = match.group().rstrip(".,;)")
        if url:
            return url
    return ""


def _get_video_url(lesson: dict) -> str:
    """
    Extract a downloadable YouTube/Vimeo URL from a lesson document.
    Checks embedUrl first, then scans description. Returns "" if none found.
    """
    embed = lesson.get("embedUrl") or ""
    if embed and _PREVIEW_URL_PATTERN.search(embed) and not _IGNORED_URL_PATTERNS.search(embed):
        return embed_to_video_url(embed)
    desc_url = _extract_url_from_description(lesson.get("description") or "")
    if desc_url and _PREVIEW_URL_PATTERN.search(desc_url):
        return embed_to_video_url(desc_url)
    return ""


def _classify_url(url: str) -> str:
    """
    Classify a lesson URL for duration estimation.
    Returns: 'transcribable' | 'non_transcribable_video' | 'zoom_recording' | 'pdf' | 'miro' | 'scheduling' | 'other' | 'none'
    """
    if not url:
        return "none"
    if re.search(r'calendly\.com|zoom\.us/j/|calendar', url, re.IGNORECASE):
        return "scheduling"
    if re.search(r'zoom\.us/(rec|clips)/', url, re.IGNORECASE):
        return "zoom_recording"
    if _PREVIEW_URL_PATTERN.search(url) and not _IGNORED_URL_PATTERNS.search(url):
        return "transcribable"
    if re.search(r'\.pdf(\?|$)', url, re.IGNORECASE):
        return "pdf"
    if re.search(r'miro\.com', url, re.IGNORECASE):
        return "miro"
    if _IGNORED_URL_PATTERNS.search(url):
        return "non_transcribable_video"
    return "other"


_YT_SCOPES       = ["https://www.googleapis.com/auth/youtube.upload"]
_YT_TOKEN_FILE   = Path(__file__).parent / "youtube-token.json"
_YT_SECRETS_FILE = Path(__file__).parent / "youtube-client-secrets.json"


def _youtube_service():
    """Return an authenticated YouTube API service, refreshing/creating the token as needed."""
    from googleapiclient.discovery import build
    from google_auth_oauthlib.flow import InstalledAppFlow
    from google.oauth2.credentials import Credentials
    from google.auth.transport.requests import Request

    creds = None
    if _YT_TOKEN_FILE.exists():
        creds = Credentials.from_authorized_user_file(str(_YT_TOKEN_FILE), _YT_SCOPES)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(str(_YT_SECRETS_FILE), _YT_SCOPES)
            creds = flow.run_local_server(port=0)
        _YT_TOKEN_FILE.write_text(creds.to_json())
    return build("youtube", "v3", credentials=creds)


def _upload_to_youtube(mp4_path: str, title: str, description: str) -> str:
    """
    Upload mp4_path to YouTube as an unlisted video.
    Returns the short YouTube URL (https://youtu.be/<id>).
    """
    from googleapiclient.http import MediaFileUpload

    youtube = _youtube_service()
    body = {
        "snippet": {
            "title": title,
            "description": description,
            "categoryId": "27",  # Education
        },
        "status": {"privacyStatus": "unlisted"},
    }
    media = MediaFileUpload(mp4_path, mimetype="video/mp4", resumable=True, chunksize=10 * 1024 * 1024)
    insert_request = youtube.videos().insert(part="snippet,status", body=body, media_body=media)

    response = None
    while response is None:
        status, response = insert_request.next_chunk()
        if status:
            print(f"      Upload: {int(status.progress() * 100)}%")

    return f"https://youtu.be/{response['id']}"


def _extract_zoom_info(description: str) -> tuple[str, str] | None:
    """
    Parse a Zoom share URL and passcode from a lesson description.
    Expected format (anywhere in the text):
        https://us06web.zoom.us/rec/share/...
        Passcode: wd71jm?=
    Returns (url, passcode) or None if not found.
    """
    zoom_url = None
    for m in re.finditer(r'https?://\S+', description or ""):
        candidate = m.group().rstrip(".,;)")
        if re.search(r'zoom\.us/rec/', candidate, re.IGNORECASE):
            zoom_url = candidate
            break
    if not zoom_url:
        return None
    pass_match = re.search(r'Passcode:\s*(\S+)', description, re.IGNORECASE)
    if not pass_match:
        return None
    # Descriptions are Markdown, so passcodes may contain backslash-escaped
    # punctuation (e.g. "drg5\*n4L" for literal "drg5*n4L"). Unescape before use.
    passcode = re.sub(r'\\(.)', r'\1', pass_match.group(1))
    return zoom_url, passcode


def _random_duration(min_minutes: int, max_minutes: int) -> dict:
    total = random.randint(min_minutes, max_minutes)
    return {"hours": total // 60, "minutes": total % 60}


def _duration_for_kind(kind: str, transcript: dict = None) -> dict:
    if kind == "transcribable" and transcript:
        return _duration_from_transcript(transcript)
    if kind in ("none", "scheduling"):
        return {"hours": 0, "minutes": 0}
    if kind in ("pdf", "miro"):
        return _random_duration(30, 60)
    if kind in ("non_transcribable_video", "zoom_recording"):
        return _random_duration(60, 90)
    return _random_duration(15, 30)  # other


def _fetch_all_lessons(course_id: str) -> list[dict]:
    """
    Return all non-quiz prod lessons for the course in topic order.
    """
    course_doc = prod_db.collection("Courses").document(course_id).get()
    if not course_doc.exists:
        raise ValueError(f"Course {course_id} not found in prod Firestore")

    topics = course_doc.to_dict().get("topics", [])
    ordered_pairs = [
        (topic["id"], item["id"])
        for topic in topics
        for item in topic.get("items", [])
        if not item.get("id", "").startswith("quiz_")
    ]

    lessons = []
    for topic_id, lesson_id in ordered_pairs:
        doc = prod_db.collection("Lessons").document(lesson_id).get()
        if not doc.exists:
            continue
        lessons.append({"id": lesson_id, "topicId": topic_id, **doc.to_dict()})
    return lessons


# ══════════════════════════════════════════════════════════════════════════════
# PER-LESSON PROCESSOR
# ══════════════════════════════════════════════════════════════════════════════

def process_lesson(course_id: str, lesson: dict):
    """
    Run all 5 content pipeline steps on a single lesson.
    Idempotent — skips steps already recorded in _PipelineState.
    """
    lesson_id = lesson["id"]
    state = get_lesson_state(course_id, lesson_id)
    done = set(state.get("stepsCompleted", []))

    video_url = _get_video_url(lesson)

    # ── Step 1: Transcription ─────────────────────────────────────────────────
    if "transcription" not in done:
        print("    [1/5] Transcribing...")
        set_lesson_state(course_id, lesson_id, {"status": "transcribing"})
        transcript = transcribe(video_url)
        write_transcript(course_id, lesson_id, transcript)
        _mark_step_done(course_id, lesson_id, "transcription")
    else:
        print("    [1/5] Transcription already done, loading...")
        transcript = prod_db.collection("Transcripts").document(f"{course_id}_{lesson_id}").get().to_dict()

    # ── Step 2: Transcript Audit ──────────────────────────────────────────────
    if "audit" not in done:
        print("    [2/5] Auditing transcript...")
        set_lesson_state(course_id, lesson_id, {"status": "auditing"})
        raw_text = transcript["fullText"]
        transcript = audit_transcript(transcript, lesson.get("title", ""))
        prod_db.collection("Transcripts").document(f"{course_id}_{lesson_id}").update({
            "rawFullText": raw_text,
            "fullText":    transcript["fullText"],
            "audited":     True,
        })
        _mark_step_done(course_id, lesson_id, "audit")
    else:
        print("    [2/5] Audit already done.")

    # ── Step 3: LLM Extraction ────────────────────────────────────────────────
    if "extraction" not in done:
        print("    [3/5] Extracting metadata...")
        set_lesson_state(course_id, lesson_id, {"status": "extracting"})
        extraction = llm_extract(transcript, lesson)
        lesson_update = {
            "shortDescription":  extraction.get("shortDescription", ""),
            "keyConcepts":       extraction.get("keyConcepts", []),
            "learningOutcomes":  extraction.get("learningOutcomes", []),
            "chapterMarkers":    extraction.get("chapterMarkers", []),
            "difficulty":        extraction.get("difficulty", ""),
            "prerequisites":     extraction.get("prerequisites", []),
            "duration":          _duration_for_kind("transcribable", transcript),
            "updatedAt": SERVER_TIMESTAMP,
        }
        if not lesson.get("description"):
            lesson_update["description"] = generate_lesson_description(transcript, lesson.get("title", ""))
        prod_db.collection("Lessons").document(lesson_id).update(lesson_update)
        _mark_step_done(course_id, lesson_id, "extraction")
    else:
        print("    [3/5] Extraction already done, loading...")
        extraction = prod_db.collection("Lessons").document(lesson_id).get().to_dict()

    # ── Step 4: Summary Generation ────────────────────────────────────────────
    if "summary" not in done:
        print("    [4/5] Generating summary...")
        set_lesson_state(course_id, lesson_id, {"status": "summarizing"})
        summary = generate_summary(transcript, extraction, lesson.get("title", ""))
        _mark_step_done(course_id, lesson_id, "summary")
    else:
        print("    [4/5] Summary already done, loading...")
        saved = prod_db.collection("LessonSummaries").document(f"{course_id}_{lesson_id}").get().to_dict() or {}
        summary = {
            "contentLatex":    saved.get("contentRaw", ""),
            "contentMarkdown": saved.get("contentMarkdownRaw", ""),
            "figureCount":     saved.get("figureCount", 0),
        }

    # ── Step 5: Figure Generation ─────────────────────────────────────────────
    if "figures" not in done:
        print(f"    [5/5] Generating {summary['figureCount']} figures...")
        set_lesson_state(course_id, lesson_id, {"status": "figures"})
        final_summary = generate_figures(course_id, lesson_id, summary)
        write_summary(course_id, lesson_id, final_summary)
        _mark_step_done(course_id, lesson_id, "figures")
    else:
        print("    [5/5] Figures already done.")

    set_lesson_state(course_id, lesson_id, {"status": "done"})
    print("    ✓ Lesson complete")


def process_zoom_lesson(course_id: str, lesson: dict):
    """
    Full pipeline for a Zoom-recorded lesson.
    Downloads the recording via Playwright, transcribes the local MP4,
    then runs audit → extraction → summary → figures identically to process_lesson.
    """
    lesson_id = lesson["id"]
    state = get_lesson_state(course_id, lesson_id)
    done = set(state.get("stepsCompleted", []))

    # ── Step 1: Download → YouTube upload → Transcription ────────────────────
    # Download is needed if either upload or transcription hasn't been done yet.
    transcript = None
    if "youtube_upload" not in done or "transcription" not in done:
        zoom_info = _extract_zoom_info(lesson.get("description", ""))
        if not zoom_info:
            raise ValueError(f"Lesson {lesson_id}: no Zoom URL/passcode found in description")
        zoom_url, zoom_password = zoom_info
        print("    [1/5] Downloading Zoom recording...")
        set_lesson_state(course_id, lesson_id, {"status": "downloading"})
        with tempfile.TemporaryDirectory() as tmpdir:
            mp4_path = download_zoom.download(
                share_url=zoom_url,
                password=zoom_password,
                output_dir=Path(tmpdir),
                headless=True,
            )

            if "youtube_upload" not in done:
                print("    [1/5] Uploading to YouTube (unlisted)...")
                set_lesson_state(course_id, lesson_id, {"status": "uploading"})
                youtube_url = _upload_to_youtube(
                    mp4_path=str(mp4_path),
                    title=lesson.get("title", lesson_id),
                    description=lesson.get("description", ""),
                )
                prod_db.collection("Lessons").document(lesson_id).update({
                    "embedUrl": youtube_url,
                    "type": "VIDEO LECTURE",
                    "updatedAt": SERVER_TIMESTAMP,
                })
                print(f"      → {youtube_url}")
                _mark_step_done(course_id, lesson_id, "youtube_upload")
            else:
                print("    [1/5] YouTube upload already done, skipping.")

            if "transcription" not in done:
                print("    [1/5] Transcribing...")
                set_lesson_state(course_id, lesson_id, {"status": "transcribing"})
                audio_path = str(Path(tmpdir) / "audio.mp3")
                subprocess.run(
                    ["ffmpeg", "-i", str(mp4_path), "-vn", "-ar", "16000", "-ac", "1", "-b:a", "32k", audio_path],
                    check=True, capture_output=True,
                )
                transcript = _transcribe_audio(audio_path)
                write_transcript(course_id, lesson_id, transcript)
                _mark_step_done(course_id, lesson_id, "transcription")
            else:
                print("    [1/5] Transcription already done, skipping.")

    # Load from Firestore if transcription was already done in a previous run
    if transcript is None:
        print("    [1/5] Transcription already done, loading...")
        transcript = prod_db.collection("Transcripts").document(f"{course_id}_{lesson_id}").get().to_dict()

    # Steps 2–5 are identical to process_lesson ──────────────────────────────

    if "audit" not in done:
        print("    [2/5] Auditing transcript...")
        set_lesson_state(course_id, lesson_id, {"status": "auditing"})
        raw_text = transcript["fullText"]
        transcript = audit_transcript(transcript, lesson.get("title", ""))
        prod_db.collection("Transcripts").document(f"{course_id}_{lesson_id}").update({
            "rawFullText": raw_text,
            "fullText":    transcript["fullText"],
            "audited":     True,
        })
        _mark_step_done(course_id, lesson_id, "audit")
    else:
        print("    [2/5] Audit already done.")

    if "extraction" not in done:
        print("    [3/5] Extracting metadata...")
        set_lesson_state(course_id, lesson_id, {"status": "extracting"})
        extraction = llm_extract(transcript, lesson)
        lesson_update = {
            "shortDescription":  extraction.get("shortDescription", ""),
            "keyConcepts":       extraction.get("keyConcepts", []),
            "learningOutcomes":  extraction.get("learningOutcomes", []),
            "chapterMarkers":    extraction.get("chapterMarkers", []),
            "difficulty":        extraction.get("difficulty", ""),
            "prerequisites":     extraction.get("prerequisites", []),
            "duration":          _duration_for_kind("transcribable", transcript),
            "updatedAt": SERVER_TIMESTAMP,
        }
        # Generate a prose description if the existing one is just a Zoom share link (no real text)
        existing_desc = lesson.get("description", "")
        if not existing_desc or re.fullmatch(r'[\s\S]*zoom\.us/rec/[\s\S]*', existing_desc, re.IGNORECASE):
            lesson_update["description"] = generate_lesson_description(transcript, lesson.get("title", ""))
        prod_db.collection("Lessons").document(lesson_id).update(lesson_update)
        _mark_step_done(course_id, lesson_id, "extraction")
    else:
        print("    [3/5] Extraction already done, loading...")
        extraction = prod_db.collection("Lessons").document(lesson_id).get().to_dict()

    if "summary" not in done:
        print("    [4/5] Generating summary...")
        set_lesson_state(course_id, lesson_id, {"status": "summarizing"})
        summary = generate_summary(transcript, extraction, lesson.get("title", ""))
        _mark_step_done(course_id, lesson_id, "summary")
    else:
        print("    [4/5] Summary already done, loading...")
        saved = prod_db.collection("LessonSummaries").document(f"{course_id}_{lesson_id}").get().to_dict() or {}
        summary = {
            "contentLatex":    saved.get("contentRaw", ""),
            "contentMarkdown": saved.get("contentMarkdownRaw", ""),
            "figureCount":     saved.get("figureCount", 0),
        }

    if "figures" not in done:
        print(f"    [5/5] Generating {summary['figureCount']} figures...")
        set_lesson_state(course_id, lesson_id, {"status": "figures"})
        final_summary = generate_figures(course_id, lesson_id, summary)
        write_summary(course_id, lesson_id, final_summary)
        _mark_step_done(course_id, lesson_id, "figures")
    else:
        print("    [5/5] Figures already done.")

    set_lesson_state(course_id, lesson_id, {"status": "done"})
    print("    ✓ Lesson complete")


# ══════════════════════════════════════════════════════════════════════════════
# COURSE ORCHESTRATOR
# ══════════════════════════════════════════════════════════════════════════════

def run_course(course_id: str):
    print(f"\n{'='*60}")
    print(f"Content pipeline: {course_id}")
    print(f"{'='*60}")

    course_state = get_course_state(course_id)
    if course_state.get("status") == "done":
        print("  Course already complete. Skipping.")
        return

    set_course_state(course_id, {"status": "processing"})

    if not prod_bucket:
        raise RuntimeError("PROD_STORAGE_BUCKET is not set — required for figure uploads")
    run_pipeline.orbit_bucket = prod_bucket  # redirect figure uploads to prod storage

    course_doc = prod_db.collection("Courses").document(course_id).get().to_dict() or {}
    course_title = course_doc.get("title", "")

    all_lessons = _fetch_all_lessons(course_id)
    if not all_lessons:
        print(f"  [WARN] No lessons found for course {course_id}")
        return

    transcribable = [l for l in all_lessons if _get_video_url(l)]
    zoom_lessons  = [l for l in all_lessons if not _get_video_url(l) and _extract_zoom_info(l.get("description", ""))]
    others        = [l for l in all_lessons if not _get_video_url(l) and not _extract_zoom_info(l.get("description", ""))]

    print(
        f"  Found {len(all_lessons)} lesson(s): "
        f"{len(transcribable)} YouTube/Vimeo, "
        f"{len(zoom_lessons)} Zoom, "
        f"{len(others)} duration-only"
    )

    # Group transcribable + zoom lesson IDs by topic for quiz generation
    topics: dict[str, list[str]] = {}
    for lesson in transcribable + zoom_lessons:
        tid = lesson["topicId"]
        topics.setdefault(tid, []).append(lesson["id"])

    # Full pipeline for YouTube/Vimeo lessons
    for lesson in transcribable:
        print(f"\n  → Lesson (YouTube/Vimeo): {lesson.get('title', lesson['id'])}")
        try:
            process_lesson(course_id, lesson)
        except Exception:
            print(f"  [ERROR] Lesson {lesson['id']} failed:")
            traceback.print_exc()
            set_lesson_state(course_id, lesson["id"], {"status": "failed"})
            set_course_state(course_id, {"status": "failed", "failedAt": lesson["id"]})
            return

    # Full pipeline for Zoom lessons
    for lesson in zoom_lessons:
        print(f"\n  → Lesson (Zoom): {lesson.get('title', lesson['id'])}")
        try:
            process_zoom_lesson(course_id, lesson)
        except Exception:
            print(f"  [ERROR] Lesson {lesson['id']} failed:")
            traceback.print_exc()
            set_lesson_state(course_id, lesson["id"], {"status": "failed"})
            set_course_state(course_id, {"status": "failed", "failedAt": lesson["id"]})
            return

    # Duration-only write for non-transcribable lessons
    for lesson in others:
        lesson_id = lesson["id"]
        state = get_lesson_state(course_id, lesson_id)
        if state.get("status") == "done":
            continue
        url  = _get_lesson_url(lesson)
        kind = _classify_url(url)
        duration = _duration_for_kind(kind)
        print(f"\n  → Lesson (duration-only, {kind}): {lesson.get('title', lesson_id)}")
        prod_db.collection("Lessons").document(lesson_id).update({
            "duration": duration,
            "updatedAt": SERVER_TIMESTAMP,
        })
        set_lesson_state(course_id, lesson_id, {"status": "done"})

    # Generate quizzes per topic
    for topic_id, lesson_ids in topics.items():
        quiz_state_key = f"quiz_topic_{topic_id}"
        if course_state.get(quiz_state_key) == "done":
            print(f"\n  Quiz for topic {topic_id} already done, skipping.")
            continue
        print(f"\n  Generating quiz for topic {topic_id} ({len(lesson_ids)} lesson(s))...")
        try:
            quiz = generate_quiz(course_id, topic_id, lesson_ids)
            write_quiz(course_id, topic_id, quiz)
            set_course_state(course_id, {quiz_state_key: "done"})
            print(f"  ✓ Quiz complete ({len(quiz['questions'])} questions)")
        except Exception:
            print(f"  [ERROR] Quiz for topic {topic_id} failed:")
            traceback.print_exc()

    # ── Duration aggregation ──────────────────────────────────────────────────
    all_lesson_ids = [l["id"] for l in all_lessons]
    total_minutes = 0
    for lid in all_lesson_ids:
        dur = (prod_db.collection("Lessons").document(lid).get().to_dict() or {}).get("duration") or {}
        total_minutes += (dur.get("hours") or 0) * 60 + (dur.get("minutes") or 0)
    prod_db.collection("Courses").document(course_id).update({
        "duration": {"hours": total_minutes // 60, "minutes": total_minutes % 60},
        "updatedAt": SERVER_TIMESTAMP,
    })
    print(f"  [DURATION] ✓ {total_minutes // 60}h {total_minutes % 60}m")

    # ── Description generation ────────────────────────────────────────────────
    print(f"\n  [DESCRIPTION] Generating course description from lesson shortDescriptions...")
    try:
        description = generate_course_description(course_id, course_title, all_lesson_ids)
        if description:
            prod_db.collection("Courses").document(course_id).update({
                "description": description,
                "updatedAt": SERVER_TIMESTAMP,
            })
            print(f"  [DESCRIPTION] ✓ Written")
        else:
            print(f"  [DESCRIPTION] [WARN] No lesson shortDescriptions available, skipping.")
    except Exception as e:
        print(f"  [DESCRIPTION] [WARN] Description generation failed: {e}")

    # ── Category population (only if missing) ────────────────────────────────
    if not course_doc.get("categoryIds"):
        print(f"\n  [CATEGORIES] categoryIds missing — generating from transcripts...")
        try:
            categories = generate_course_categories(course_id, course_title, topics)
            prod_db.collection("Courses").document(course_id).update({
                "categoryIds": categories,
                "updatedAt": SERVER_TIMESTAMP,
            })
            print(f"  [CATEGORIES] ✓ {categories}")
        except Exception as e:
            print(f"  [CATEGORIES] [WARN] Category generation failed: {e}")

    # ── Slug generation (only if missing) ────────────────────────────────────
    if not course_doc.get("slug"):
        print(f"\n  [SLUG] slug missing — generating...")
        try:
            slug = generate_course_slug(course_title)
            prod_db.collection("Courses").document(course_id).update({
                "slug": slug,
                "updatedAt": SERVER_TIMESTAMP,
            })
            print(f"  [SLUG] ✓ {slug}")
        except Exception as e:
            print(f"  [SLUG] [WARN] Slug generation failed: {e}")

    # ── Mode ──────────────────────────────────────────────────────────────────
    prod_db.collection("Courses").document(course_id).update({
        "mode": "SELF-PACED",
        "updatedAt": SERVER_TIMESTAMP,
    })

    # ── Tag population (only if missing) ─────────────────────────────────────
    if not course_doc.get("tags"):
        print(f"\n  [TAGS] tags missing — generating from transcripts...")
        try:
            tags = generate_course_tags(course_id, course_title, topics)
            prod_db.collection("Courses").document(course_id).update({
                "tags": tags,
                "updatedAt": SERVER_TIMESTAMP,
            })
            print(f"  [TAGS] ✓ {tags}")
        except Exception as e:
            print(f"  [TAGS] [WARN] Tag generation failed: {e}")

    set_course_state(course_id, {"status": "done"})
    print(f"\n  ✓ Course {course_id} complete")


# ══════════════════════════════════════════════════════════════════════════════
# ENTRY POINT
# ══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    import json as _json

    args = sys.argv[1:]
    if args and args[0] == "--from-json":
        json_path = args[1] if len(args) > 1 else "courses.json"
        with open(json_path) as f:
            ids = [c["firestoreId"] for c in _json.load(f) if c.get("firestoreId")]
        print(f"Loaded {len(ids)} course IDs from {json_path}")
    else:
        ids = args or COURSE_IDS

    if not ids:
        print("No course IDs provided. Add them to COURSE_IDS or pass as arguments.")
        sys.exit(1)
    for cid in ids:
        run_course(cid)
