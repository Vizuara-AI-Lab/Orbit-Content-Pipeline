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

import sys
import uuid
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



# ══════════════════════════════════════════════════════════════════════════════
# HELPERS
# ══════════════════════════════════════════════════════════════════════════════

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


def _fetch_valid_lessons(course_id: str) -> list[dict]:
    """
    Return prod lessons for the course that have a resolvable YouTube/Vimeo URL.
    Preserves topic order via the Courses document's topics array.
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

    valid = []
    for topic_id, lesson_id in ordered_pairs:
        doc = prod_db.collection("Lessons").document(lesson_id).get()
        if not doc.exists:
            continue
        lesson = {"id": lesson_id, "topicId": topic_id, **doc.to_dict()}
        if _get_video_url(lesson):
            valid.append(lesson)

    return valid


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
            "shortDescription":       extraction.get("shortDescription", ""),
            "keyConcepts":            extraction.get("keyConcepts", []),
            "learningOutcomes":       extraction.get("learningOutcomes", []),
            "chapterMarkers":         extraction.get("chapterMarkers", []),
            "difficulty":             extraction.get("difficulty", ""),
            "estimatedDurationHours": extraction.get("estimatedDurationHours", 0),
            "prerequisites":          extraction.get("prerequisites", []),
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

    lessons = _fetch_valid_lessons(course_id)
    if not lessons:
        print(f"  [WARN] No valid lessons found for course {course_id}")
        return

    print(f"  Found {len(lessons)} valid lesson(s)")

    # Group lesson IDs by topic (preserving order) for quiz generation
    topics: dict[str, list[str]] = {}
    for lesson in lessons:
        tid = lesson["topicId"]
        topics.setdefault(tid, []).append(lesson["id"])

    # Process lessons
    for lesson in lessons:
        print(f"\n  → Lesson: {lesson.get('title', lesson['id'])}")
        try:
            process_lesson(course_id, lesson)
        except Exception:
            print(f"  [ERROR] Lesson {lesson['id']} failed:")
            traceback.print_exc()
            set_lesson_state(course_id, lesson["id"], {"status": "failed"})
            set_course_state(course_id, {"status": "failed", "failedAt": lesson["id"]})
            return

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
