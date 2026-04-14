#!/usr/bin/env python3
"""
Vizuara Orbit — Vimeo Migration Pipeline

For each lesson whose embedUrl is a Vimeo link:
  1. Downloads the video via yt-dlp
  2. Uploads to YouTube (unlisted) → writes Lesson.embedUrl + type
  3. Records progress in _PipelineStateProd (resumable)

No transcription, summaries, or figures are generated.

Usage:
    python run_vimeo_pipeline.py <course_id> [<course_id> ...]
    python run_vimeo_pipeline.py --from-json [courses.json]

    If no course IDs are passed, runs all IDs in COURSE_IDS below.
"""

import json as _json
import re
import subprocess
import sys
import tempfile
import traceback
from pathlib import Path

from google.cloud.firestore_v1 import SERVER_TIMESTAMP

import run_pipeline
from run_pipeline import prod_db

# ─── Course IDs to process ────────────────────────────────────────────────────
COURSE_IDS: list[str] = [
    # "course_id_01",
]

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


def _upload_to_youtube(mp4_path: str, title: str) -> str:
    """Upload mp4_path to YouTube as an unlisted video. Returns the short YouTube URL."""
    from googleapiclient.http import MediaFileUpload

    youtube = _youtube_service()
    body = {
        "snippet": {
            "title": title,
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


# ── Vimeo URL detection ───────────────────────────────────────────────────────

def _is_vimeo_url(url: str) -> bool:
    return bool(re.search(r'vimeo\.com/', url or "", re.IGNORECASE))


# ── Vimeo download ────────────────────────────────────────────────────────────

def _download_vimeo(vimeo_url: str, output_dir: Path) -> Path:
    """Download a Vimeo video via yt-dlp. Returns path to the downloaded mp4."""
    output_template = str(output_dir / "video.%(ext)s")
    subprocess.run(
        [
            "yt-dlp",
            "--format", "bestvideo[ext=mp4]+bestaudio[ext=m4a]/best[ext=mp4]/best",
            "--merge-output-format", "mp4",
            "--output", output_template,
            "--no-playlist",
            vimeo_url,
        ],
        check=True,
    )
    # Find the downloaded file
    matches = list(output_dir.glob("video.*"))
    if not matches:
        raise FileNotFoundError(f"yt-dlp did not produce a video file in {output_dir}")
    return matches[0]


# ── Pipeline state ────────────────────────────────────────────────────────────

def _get_state(course_id: str, lesson_id: str) -> dict:
    doc = (
        prod_db.collection("_PipelineStateProd")
        .document(course_id)
        .collection("Lessons")
        .document(lesson_id)
        .get()
    )
    return doc.to_dict() or {} if doc.exists else {}


def _set_state(course_id: str, lesson_id: str, data: dict):
    (
        prod_db.collection("_PipelineStateProd")
        .document(course_id)
        .collection("Lessons")
        .document(lesson_id)
        .set({**data, "updatedAt": SERVER_TIMESTAMP}, merge=True)
    )


def _mark_step_done(course_id: str, lesson_id: str, step: str):
    state = _get_state(course_id, lesson_id)
    done = list(set(state.get("stepsCompleted", []) + [step]))
    _set_state(course_id, lesson_id, {"stepsCompleted": done})


# ── Lesson fetcher ────────────────────────────────────────────────────────────

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

def process_vimeo_lesson(course_id: str, lesson: dict):
    """
    Download a Vimeo lesson and upload it to YouTube.
    Idempotent — skips if youtube_upload already recorded in _PipelineStateProd.
    """
    lesson_id = lesson["id"]

    vimeo_url = lesson.get("embedUrl", "")
    if not _is_vimeo_url(vimeo_url):
        raise ValueError(f"Lesson {lesson_id}: embedUrl is not a Vimeo URL: {vimeo_url!r}")

    print(f"    [1/1] Downloading from Vimeo: {vimeo_url}")
    _set_state(course_id, lesson_id, {"status": "downloading"})

    with tempfile.TemporaryDirectory() as tmpdir:
        mp4_path = _download_vimeo(vimeo_url, Path(tmpdir))

        print("    [1/1] Uploading to YouTube (unlisted)...")
        _set_state(course_id, lesson_id, {"status": "uploading"})
        youtube_url = _upload_to_youtube(
            mp4_path=str(mp4_path),
            title=lesson.get("title", lesson_id),
        )

    prod_db.collection("Lessons").document(lesson_id).update({
        "embedUrl": youtube_url,
        "type": "VIDEO LECTURE",
        "updatedAt": SERVER_TIMESTAMP,
    })
    print(f"      → {youtube_url}")
    _mark_step_done(course_id, lesson_id, "youtube_upload")
    _set_state(course_id, lesson_id, {"status": "done"})
    print("    ✓ Lesson complete")


# ══════════════════════════════════════════════════════════════════════════════
# COURSE ORCHESTRATOR
# ══════════════════════════════════════════════════════════════════════════════

def run_vimeo_course(course_id: str):
    print(f"\n{'='*60}")
    print(f"Vimeo pipeline: {course_id}")
    print(f"{'='*60}")

    all_lessons = _fetch_all_lessons(course_id)
    if not all_lessons:
        print(f"  [WARN] No lessons found for course {course_id}")
        return

    vimeo_lessons = [l for l in all_lessons if _is_vimeo_url(l.get("embedUrl", ""))]

    if not vimeo_lessons:
        print("  No Vimeo lessons found.")
        return

    print(f"  Found {len(vimeo_lessons)} Vimeo lesson(s)")

    for lesson in vimeo_lessons:
        print(f"\n  → {lesson.get('title', lesson['id'])}")
        try:
            process_vimeo_lesson(course_id, lesson)
        except Exception:
            print(f"  [ERROR] Lesson {lesson['id']} failed:")
            traceback.print_exc()


# ══════════════════════════════════════════════════════════════════════════════
# ENTRY POINT
# ══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    args = sys.argv[1:]
    if args and args[0] == "--from-json":
        json_path = args[1] if len(args) > 1 else "courses.json"
        with open(json_path) as f:
            ids = [c["firestoreId"] for c in _json.load(f) if c.get("firestoreId")]
        print(f"Loaded {len(ids)} course ID(s) from {json_path}")
    else:
        ids = args or COURSE_IDS

    if not ids:
        print("Usage: python run_vimeo_pipeline.py <course_id> [...]")
        print("       python run_vimeo_pipeline.py --from-json [courses.json]")
        sys.exit(1)

    for cid in ids:
        run_vimeo_course(cid)
