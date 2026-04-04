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

import re
import sys
import uuid
import random
import traceback

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
    _llm_json_array,
    QUIZ_SYSTEM,
    MAX_CHARS_PER_LESSON,
    transcribe,
    audit_transcript,
    llm_extract,
    generate_summary,
    generate_figures,
)


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
    doc = orbit_db.collection("_PipelineStateProd").document(course_id).get()
    return doc.to_dict() or {} if doc.exists else {}


def set_course_state(course_id: str, data: dict):
    orbit_db.collection("_PipelineStateProd").document(course_id).set(
        {**data, "updatedAt": SERVER_TIMESTAMP}, merge=True
    )


def get_lesson_state(course_id: str, lesson_id: str) -> dict:
    doc = (
        orbit_db.collection("_PipelineStateProd")
        .document(course_id)
        .collection("Lessons")
        .document(lesson_id)
        .get()
    )
    return doc.to_dict() or {} if doc.exists else {}


def set_lesson_state(course_id: str, lesson_id: str, data: dict):
    (
        orbit_db.collection("_PipelineStateProd")
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
    return _extract_url_from_description(lesson.get("description") or "")


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
    Returns: 'transcribable' | 'non_transcribable_video' | 'pdf' | 'miro' | 'other' | 'none'
    """
    if not url:
        return "none"
    if _PREVIEW_URL_PATTERN.search(url) and not _IGNORED_URL_PATTERNS.search(url):
        return "transcribable"
    if re.search(r'\.pdf(\?|$)', url, re.IGNORECASE):
        return "pdf"
    if re.search(r'miro\.com', url, re.IGNORECASE):
        return "miro"
    if _IGNORED_URL_PATTERNS.search(url):
        return "non_transcribable_video"
    return "other"


def _random_duration(min_minutes: int, max_minutes: int) -> dict:
    total = random.randint(min_minutes, max_minutes)
    return {"hours": total // 60, "minutes": total % 60}


def _duration_for_kind(kind: str, transcript: dict = None) -> dict:
    if kind == "transcribable" and transcript:
        return _duration_from_transcript(transcript)
    if kind == "none":
        return {"hours": 0, "minutes": 0}
    if kind in ("pdf", "miro"):
        return _random_duration(30, 60)
    if kind == "non_transcribable_video":
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
        prod_db.collection("Lessons").document(lesson_id).update({
            "shortDescription":  extraction.get("shortDescription", ""),
            "keyConcepts":       extraction.get("keyConcepts", []),
            "learningOutcomes":  extraction.get("learningOutcomes", []),
            "chapterMarkers":    extraction.get("chapterMarkers", []),
            "difficulty":        extraction.get("difficulty", ""),
            "prerequisites":     extraction.get("prerequisites", []),
            "duration":          _duration_for_kind("transcribable", transcript),
            "updatedAt": SERVER_TIMESTAMP,
        })
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
    others        = [l for l in all_lessons if not _get_video_url(l)]

    print(f"  Found {len(all_lessons)} lesson(s): {len(transcribable)} transcribable, {len(others)} duration-only")

    # Group transcribable lesson IDs by topic for quiz generation
    topics: dict[str, list[str]] = {}
    for lesson in transcribable:
        tid = lesson["topicId"]
        topics.setdefault(tid, []).append(lesson["id"])

    # Full pipeline for transcribable lessons
    for lesson in transcribable:
        print(f"\n  → Lesson: {lesson.get('title', lesson['id'])}")
        try:
            process_lesson(course_id, lesson)
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
    ids = sys.argv[1:] or COURSE_IDS
    if not ids:
        print("No course IDs provided. Add them to COURSE_IDS or pass as arguments.")
        sys.exit(1)
    for cid in ids:
        run_course(cid)
