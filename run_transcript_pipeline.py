#!/usr/bin/env python3
"""
Vizuara Orbit - Transcript-only Pipeline

Creates transcripts for production course lesson lecture videos and skips lessons
that already have transcript documents.

Usage:
    python run_transcript_pipeline.py <course_id> [<course_id> ...]
    python run_transcript_pipeline.py --from-json [courses-1.json]
    python run_transcript_pipeline.py --workers 4 --from-json courses-1.json

The JSON file may contain either {"id": "..."} or {"firestoreId": "..."} entries.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import subprocess
import tempfile
import traceback
import html
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path

_VENV_BIN = Path(sys.executable).parent
YT_DLP = str(_VENV_BIN / "yt-dlp")

prod_db = None
oai_client = None
SERVER_TIMESTAMP = None


def _init_clients():
    global prod_db, oai_client, SERVER_TIMESTAMP
    if prod_db is not None and oai_client is not None:
        return
    import firebase_admin
    import openai
    from dotenv import load_dotenv
    from firebase_admin import credentials, firestore
    from google.cloud.firestore_v1 import SERVER_TIMESTAMP as firestore_server_timestamp

    load_dotenv()
    prod_sa = os.getenv("PROD_SERVICE_ACCOUNT", "prod-service-account.json")
    try:
        prod_app = firebase_admin.get_app("prod-transcripts")
    except ValueError:
        prod_app = firebase_admin.initialize_app(
            credentials.Certificate(prod_sa),
            name="prod-transcripts",
        )
    prod_db = firestore.client(app=prod_app)
    oai_client = openai.OpenAI(api_key=os.environ["OPENAI_API_KEY"])
    SERVER_TIMESTAMP = firestore_server_timestamp

_IGNORED_URL_PATTERNS = re.compile(
    r"(calendar\.google\.com|discord\.(gg|com)|senja\.io|zoom\.us|veed\.io|drive\.google\.com|arxiv\.org)",
    re.IGNORECASE,
)
_PREVIEW_URL_PATTERN = re.compile(r"(youtube\.com|youtu\.be|vimeo\.com)", re.IGNORECASE)


DEFAULT_WORKERS = 3


@dataclass(frozen=True)
class TranscriptJob:
    course_id: str
    topic_id: str
    lesson_id: str
    lesson_title: str
    source_kind: str
    video_url: str = ""
    zoom_passcode: str = ""


def _extract_url_from_description(description: str) -> str:
    """Return the first usable YouTube/Vimeo candidate from a lesson description."""
    for match in re.finditer(r"https?://\S+", description or ""):
        url = match.group().rstrip(".,;)")
        if _PREVIEW_URL_PATTERN.search(url) and not _IGNORED_URL_PATTERNS.search(url):
            return url
    return ""


def _extract_zoom_info(description: str) -> tuple[str, str] | None:
    """
    Parse a Zoom recording URL and passcode from a lesson description.
    Expected anywhere in the text:
      https://...zoom.us/rec/share/...
      Passcode: abc123
    """
    zoom_url = None
    for match in re.finditer(r"https?://\S+", description or ""):
        candidate = match.group().rstrip(".,;)")
        if re.search(r"zoom\.us/(rec|clips)/", candidate, re.IGNORECASE):
            zoom_url = candidate
            break
    if not zoom_url:
        return None

    pass_match = re.search(r"Passcode:\s*(\S+)", description or "", re.IGNORECASE)
    if not pass_match:
        return None

    passcode = html.unescape(re.sub(r"\\(.)", r"\1", pass_match.group(1)))
    return zoom_url, passcode


def embed_to_video_url(embed: str) -> str:
    """Convert common embed URLs to URLs yt-dlp can download."""
    if not embed:
        return ""
    yt_match = re.search(r"youtube\.com/embed/([^?&/]+)", embed)
    if yt_match:
        return f"https://www.youtube.com/watch?v={yt_match.group(1)}"
    vimeo_match = re.search(r"vimeo\.com/video/(\d+)", embed)
    if vimeo_match:
        return f"https://vimeo.com/{vimeo_match.group(1)}"
    return embed


def transcribe(video_url: str) -> dict:
    with tempfile.TemporaryDirectory() as tmpdir:
        audio_path = _download_audio(video_url, tmpdir)
        return _transcribe_audio(audio_path)


def transcribe_zoom(share_url: str, passcode: str) -> dict:
    import download_zoom

    with tempfile.TemporaryDirectory() as tmpdir:
        try:
            mp4_path = download_zoom.download(
                share_url=share_url,
                password=passcode,
                output_dir=Path(tmpdir),
                headless=True,
            )
        except SystemExit as exc:
            raise RuntimeError(f"Zoom download exited with code {exc.code}") from exc
        audio_path = str(Path(tmpdir) / "audio.mp3")
        subprocess.run(
            ["ffmpeg", "-i", str(mp4_path), "-vn", "-ar", "16000", "-ac", "1", "-b:a", "32k", audio_path],
            check=True,
            capture_output=True,
        )
        return _transcribe_audio(audio_path)


def _download_audio(url: str, output_dir: str) -> str:
    if _is_direct_file(url):
        output_path = os.path.join(output_dir, "audio.mp3")
        import urllib.request

        urllib.request.urlretrieve(url, output_path)
        return output_path

    cookies_file = Path(__file__).parent / "yt-cookies.txt"
    cmd = [
        YT_DLP,
        "--format",
        "bestaudio",
        "--output",
        "audio.%(ext)s",
        "--no-playlist",
        "--fixup",
        "never",
        "--remote-components",
        "ejs:github",
    ]
    if cookies_file.exists():
        cmd += ["--cookies", str(cookies_file)]
    yt_visitor_data = os.environ.get("YT_VISITOR_DATA")
    if yt_visitor_data:
        cmd += ["--extractor-args", f"youtube:visitor_data={yt_visitor_data}"]
    cmd.append(url)

    # Match the main content pipeline: yt-dlp may need Deno for YouTube's
    # n-challenge solver on the deployment host.
    env = {**os.environ, "PATH": os.environ.get("PATH", "") + os.pathsep + "/home/teamvizuara/.deno/bin"}
    result = subprocess.run(cmd, capture_output=True, text=True, cwd=output_dir, env=env)
    if result.returncode != 0:
        raise RuntimeError(f"yt-dlp failed: {result.stderr[:1000]}")

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
            timestamp_granularities=["segment"],
        )
    segments = [{"start": s.start, "end": s.end, "text": s.text.strip()} for s in resp.segments if s.text.strip()]
    return {"segments": segments, "fullText": " ".join(s["text"] for s in segments)}


def _transcribe_chunked(audio_path: str) -> dict:
    chunk_dir = Path(audio_path).parent / "chunks"
    chunk_dir.mkdir(exist_ok=True)
    subprocess.run(
        [
            "ffmpeg",
            "-i",
            audio_path,
            "-vn",
            "-ar",
            "16000",
            "-ac",
            "1",
            "-b:a",
            "32k",
            "-f",
            "segment",
            "-segment_time",
            "600",
            str(chunk_dir / "chunk_%03d.mp3"),
        ],
        check=True,
        capture_output=True,
    )
    all_segments, offset = [], 0.0
    for chunk in sorted(chunk_dir.glob("chunk_*.mp3")):
        result = _transcribe_single(str(chunk))
        for seg in result["segments"]:
            all_segments.append({**seg, "start": seg["start"] + offset, "end": seg["end"] + offset})
        if result["segments"]:
            offset = all_segments[-1]["end"]
    return {"segments": all_segments, "fullText": " ".join(s["text"] for s in all_segments)}


def _fetch_all_lessons(course_id: str) -> list[dict]:
    """Return all non-quiz prod lessons for the course in topic order."""
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
        if doc.exists:
            lessons.append({"id": lesson_id, "topicId": topic_id, **doc.to_dict()})
    return lessons


def _get_video_url(lesson: dict) -> str:
    """
    Extract a transcribable YouTube/Vimeo URL from a lesson document.
    Checks embedUrl first, then scans description.
    """
    embed = lesson.get("embedUrl") or ""
    if embed and _PREVIEW_URL_PATTERN.search(embed) and not _IGNORED_URL_PATTERNS.search(embed):
        return embed_to_video_url(embed)

    desc_url = _extract_url_from_description(lesson.get("description") or "")
    if desc_url and _PREVIEW_URL_PATTERN.search(desc_url):
        return embed_to_video_url(desc_url)
    return ""


def _has_transcript(course_id: str, lesson_id: str) -> bool:
    doc = prod_db.collection("Transcripts").document(f"{course_id}_{lesson_id}").get()
    if not doc.exists:
        return False
    data = doc.to_dict() or {}
    return bool(data.get("fullText") or data.get("segments"))


def _set_lesson_state(course_id: str, lesson_id: str, data: dict):
    (
        prod_db.collection("_PipelineStateProd")
        .document(course_id)
        .collection("Lessons")
        .document(lesson_id)
        .set({**data, "updatedAt": SERVER_TIMESTAMP}, merge=True)
    )


def _mark_step_done(course_id: str, lesson_id: str, step: str):
    ref = (
        prod_db.collection("_PipelineStateProd")
        .document(course_id)
        .collection("Lessons")
        .document(lesson_id)
    )
    doc = ref.get()
    state = doc.to_dict() or {} if doc.exists else {}
    done = sorted(set(state.get("stepsCompleted", []) + [step]))
    ref.set({"stepsCompleted": done, "updatedAt": SERVER_TIMESTAMP}, merge=True)


def _write_transcript(course_id: str, lesson_id: str, transcript: dict):
    prod_db.collection("Transcripts").document(f"{course_id}_{lesson_id}").set(
        {
            "courseId": course_id,
            "lessonId": lesson_id,
            **transcript,
            "createdAt": SERVER_TIMESTAMP,
        }
    )


def _build_jobs(course_ids: list[str]) -> tuple[list[TranscriptJob], dict[str, int]]:
    jobs: list[TranscriptJob] = []
    queued: set[tuple[str, str]] = set()
    stats = {
        "courses": len(course_ids),
        "lessons": 0,
        "youtube_vimeo_lessons": 0,
        "zoom_lessons": 0,
        "existing_transcripts": 0,
        "missing_transcripts": 0,
    }

    for course_id in course_ids:
        lessons = _fetch_all_lessons(course_id)
        stats["lessons"] += len(lessons)

        for lesson in lessons:
            video_url = _get_video_url(lesson)
            zoom_info = _extract_zoom_info(lesson.get("description", "")) if not video_url else None
            if not video_url and not zoom_info:
                continue

            if video_url:
                stats["youtube_vimeo_lessons"] += 1
                source_kind = "youtube_vimeo"
                zoom_url = ""
                zoom_passcode = ""
            else:
                stats["zoom_lessons"] += 1
                source_kind = "zoom"
                zoom_url, zoom_passcode = zoom_info

            lesson_id = lesson["id"]
            if _has_transcript(course_id, lesson_id):
                stats["existing_transcripts"] += 1
                continue
            if (course_id, lesson_id) in queued:
                continue

            stats["missing_transcripts"] += 1
            queued.add((course_id, lesson_id))
            jobs.append(
                TranscriptJob(
                    course_id=course_id,
                    topic_id=lesson.get("topicId", ""),
                    lesson_id=lesson_id,
                    lesson_title=lesson.get("title", lesson_id),
                    source_kind=source_kind,
                    video_url=video_url or zoom_url,
                    zoom_passcode=zoom_passcode,
                )
            )

    return jobs, stats


def _run_job(job: TranscriptJob) -> str:
    label = f"{job.course_id}/{job.lesson_id}"
    if _has_transcript(job.course_id, job.lesson_id):
        return f"SKIP {label}: transcript appeared before worker started"

    _set_lesson_state(job.course_id, job.lesson_id, {"status": "transcribing"})
    if job.source_kind == "zoom":
        transcript = transcribe_zoom(job.video_url, job.zoom_passcode)
    else:
        transcript = transcribe(job.video_url)
    _write_transcript(job.course_id, job.lesson_id, transcript)
    _mark_step_done(job.course_id, job.lesson_id, "transcription")
    _set_lesson_state(job.course_id, job.lesson_id, {"status": "transcript_done"})
    return f"DONE {label} ({job.source_kind}): {job.lesson_title}"


def _load_course_ids_from_json(path: str) -> list[str]:
    with open(path, encoding="utf-8") as f:
        raw = json.load(f)

    ids: list[str] = []
    for entry in raw:
        if isinstance(entry, str):
            ids.append(entry)
        elif isinstance(entry, dict):
            course_id = entry.get("id") or entry.get("firestoreId")
            if course_id:
                ids.append(course_id)
    return ids


def _parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Create only missing transcripts for course lecture videos.")
    parser.add_argument("course_ids", nargs="*", help="Course IDs to process.")
    parser.add_argument("--from-json", nargs="?", const="courses-1.json", help="Load course IDs from JSON.")
    parser.add_argument("--workers", type=int, default=DEFAULT_WORKERS, help=f"Parallel workers. Default: {DEFAULT_WORKERS}.")
    return parser.parse_args(argv)


def main(argv: list[str]) -> int:
    args = _parse_args(argv)
    course_ids = _load_course_ids_from_json(args.from_json) if args.from_json else args.course_ids
    course_ids = [cid for cid in course_ids if cid]

    if not course_ids:
        print("No course IDs provided. Pass IDs or use --from-json courses-1.json.")
        return 1

    workers = max(1, args.workers)
    _init_clients()

    print(f"Transcript-only pipeline")
    print(f"Courses: {len(course_ids)}")
    print(f"Workers: {workers}")

    jobs, stats = _build_jobs(course_ids)
    print(
        "Scan complete: "
        f"{stats['lessons']} lesson(s), "
        f"{stats['youtube_vimeo_lessons']} YouTube/Vimeo lecture video(s), "
        f"{stats['zoom_lessons']} Zoom recording(s), "
        f"{stats['existing_transcripts']} existing transcript(s), "
        f"{stats['missing_transcripts']} missing transcript(s)."
    )

    if not jobs:
        print("Nothing to do.")
        return 0

    failures = 0
    with ThreadPoolExecutor(max_workers=workers) as executor:
        future_to_job = {executor.submit(_run_job, job): job for job in jobs}
        for future in as_completed(future_to_job):
            job = future_to_job[future]
            try:
                print(future.result())
            except Exception:
                failures += 1
                print(f"FAIL {job.course_id}/{job.lesson_id}: {job.lesson_title}")
                traceback.print_exc()
                _set_lesson_state(job.course_id, job.lesson_id, {"status": "transcript_failed"})

    print(f"Finished: {len(jobs) - failures} created, {failures} failed.")
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
