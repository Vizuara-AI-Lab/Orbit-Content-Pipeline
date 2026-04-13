#!/usr/bin/env python3
"""
Vizuara Orbit — Zoom Recording Pipeline

Processes Zoom-recorded lessons within existing courses.
Works on courses already processed by run_content_pipeline.py — not blocked
by course "done" status.

For each lesson whose description contains a Zoom share URL + passcode:
  1. Downloads the recording via Playwright
  2. Uploads to YouTube (unlisted) → writes Lesson.embedUrl
  3. Transcribes the local MP4 via Whisper
  4. Audits transcript
  5. Extracts metadata + generates prose description
  6. Generates summary + figures
  7. Generates quiz per topic (only if one doesn't already exist)

Fully resumable — each step is gated by _PipelineStateProd.

Usage:
    python run_zoom_pipeline.py <course_id> [<course_id> ...]

    If no course IDs are passed, runs all IDs in COURSE_IDS below.
"""

import os
import re
import sys
import tempfile
import traceback
from pathlib import Path

from google.cloud.firestore_v1 import SERVER_TIMESTAMP

import run_pipeline
import download_zoom
from run_pipeline import (
    prod_db,
    prod_bucket,
    _transcribe_audio,
    _llm_raw,
    audit_transcript,
    llm_extract,
    generate_summary,
    generate_figures,
    FIGURE_STYLE_CONTEXT,
)

# ─── Course IDs to process ────────────────────────────────────────────────────
COURSE_IDS: list[str] = [
    # "course_id_01",
]

# ── Google API key rotation ───────────────────────────────────────────────────
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

    raise RuntimeError("All Google API keys exhausted") from last_exc


run_pipeline._paperbanana_and_upload = _paperbanana_and_upload_with_fallback


# ── YouTube upload ────────────────────────────────────────────────────────────
_YT_SCOPES       = ["https://www.googleapis.com/auth/youtube.upload"]
_YT_TOKEN_FILE   = Path(__file__).parent / "youtube-token.json"
_YT_SECRETS_FILE = Path(__file__).parent / "youtube-client-secrets.json"


def _youtube_service():
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
    """Upload mp4_path to YouTube as an unlisted video. Returns the short YouTube URL."""
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


# ── Zoom URL + passcode parser ────────────────────────────────────────────────

def _extract_zoom_info(description: str) -> tuple[str, str] | None:
    """
    Parse a Zoom share URL and passcode from a lesson description.
    Expected format:
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
    return zoom_url, pass_match.group(1)


# ── Pipeline state ────────────────────────────────────────────────────────────

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


# ── Firestore writers ─────────────────────────────────────────────────────────

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


# ── Helpers ───────────────────────────────────────────────────────────────────

def _duration_from_transcript(transcript: dict) -> dict:
    segments = transcript.get("segments", [])
    total_seconds = segments[-1]["end"] if segments else 0
    return {"hours": int(total_seconds // 3600), "minutes": int((total_seconds % 3600) // 60)}


LESSON_DESCRIPTION_SYSTEM = """
You are a curriculum designer. Given a lesson title and its transcript, write a
5-6 sentence description of the lesson — what it covers, what students will learn,
and why it matters.

Return ONLY the description as plain text, no JSON, no markdown.
"""


def _generate_lesson_description(transcript: dict, lesson_title: str) -> str:
    full_text = transcript.get("fullText", "")[:12000]
    user = f"Lesson title: {lesson_title}\n\nTranscript:\n{full_text}"
    return _llm_raw(LESSON_DESCRIPTION_SYSTEM, user, max_tokens=512).strip()



def _fetch_all_lessons(course_id: str) -> list[dict]:
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

def process_zoom_lesson(course_id: str, lesson: dict):
    """
    Full pipeline for a single Zoom-recorded lesson.
    Idempotent — skips steps already recorded in _PipelineStateProd.
    """
    lesson_id = lesson["id"]
    state = get_lesson_state(course_id, lesson_id)
    done = set(state.get("stepsCompleted", []))

    # ── Step 1: Download → YouTube upload → Transcription ────────────────────
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
                    "updatedAt": SERVER_TIMESTAMP,
                })
                print(f"      → {youtube_url}")
                _mark_step_done(course_id, lesson_id, "youtube_upload")
            else:
                print("    [1/5] YouTube upload already done, skipping.")

            if "transcription" not in done:
                print("    [1/5] Transcribing...")
                set_lesson_state(course_id, lesson_id, {"status": "transcribing"})
                transcript = _transcribe_audio(str(mp4_path))
                write_transcript(course_id, lesson_id, transcript)
                _mark_step_done(course_id, lesson_id, "transcription")
            else:
                print("    [1/5] Transcription already done, skipping.")

    if transcript is None:
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
            "duration":          _duration_from_transcript(transcript),
            "updatedAt": SERVER_TIMESTAMP,
        }
        existing_desc = lesson.get("description", "")
        if not existing_desc or re.search(r'zoom\.us/rec/', existing_desc, re.IGNORECASE):
            generated = _generate_lesson_description(transcript, lesson.get("title", ""))
            lesson_update["description"] = f"{existing_desc}\n\n{generated}".strip()
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


# ══════════════════════════════════════════════════════════════════════════════
# COURSE ORCHESTRATOR
# ══════════════════════════════════════════════════════════════════════════════

def run_zoom_course(course_id: str):
    print(f"\n{'='*60}")
    print(f"Zoom pipeline: {course_id}")
    print(f"{'='*60}")

    if not prod_bucket:
        raise RuntimeError("PROD_STORAGE_BUCKET is not set — required for figure uploads")
    run_pipeline.orbit_bucket = prod_bucket  # redirect figure uploads to prod storage

    all_lessons = _fetch_all_lessons(course_id)
    if not all_lessons:
        print(f"  [WARN] No lessons found for course {course_id}")
        return

    zoom_lessons = [
        l for l in all_lessons
        if _extract_zoom_info(l.get("description", ""))
    ]

    if not zoom_lessons:
        print("  No Zoom lessons found.")
        return

    print(f"  Found {len(zoom_lessons)} Zoom lesson(s)")

    for lesson in zoom_lessons:
        print(f"\n  → {lesson.get('title', lesson['id'])}")
        try:
            process_zoom_lesson(course_id, lesson)
        except Exception:
            print(f"  [ERROR] Lesson {lesson['id']} failed:")
            traceback.print_exc()



# ══════════════════════════════════════════════════════════════════════════════
# ENTRY POINT
# ══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    ids = sys.argv[1:] or COURSE_IDS
    if not ids:
        print("Usage: python run_zoom_pipeline.py <course_id> [<course_id> ...]")
        sys.exit(1)
    for cid in ids:
        run_zoom_course(cid)
