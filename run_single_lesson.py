#!/usr/bin/env python3
"""
Vizuara Orbit — Single-Lesson Content Pipeline

Runs the content pipeline (transcribe → audit → extract → summary → figures)
for ONE lesson only. No quiz, no course-level aggregation.

Auto-detects the source:
  - YouTube embedUrl      → process_lesson
  - Vimeo embedUrl        → migrate to YouTube via process_vimeo_lesson,
                             then process_lesson on the updated lesson
  - Zoom URL in description → process_zoom_lesson

Usage:
    python run_single_lesson.py <course_id> <lesson_id>
"""

import sys
import traceback

import run_pipeline
from run_pipeline import prod_db, prod_bucket
from run_content_pipeline import (
    process_lesson,
    process_zoom_lesson,
    _extract_zoom_info,
)
from run_vimeo_pipeline import process_vimeo_lesson, _is_vimeo_url


def _load_lesson(lesson_id: str, course_id: str) -> dict:
    doc = prod_db.collection("Lessons").document(lesson_id).get()
    if not doc.exists:
        raise ValueError(f"Lesson {lesson_id} not found in prod Firestore")
    # topicId is only needed by course-level code, not by process_lesson itself.
    return {"id": lesson_id, "topicId": None, **doc.to_dict()}


def run_single_lesson(course_id: str, lesson_id: str):
    if not prod_bucket:
        raise RuntimeError("PROD_STORAGE_BUCKET is not set — required for figure uploads")
    run_pipeline.orbit_bucket = prod_bucket  # route figure uploads to prod storage

    lesson = _load_lesson(lesson_id, course_id)
    embed_url = lesson.get("embedUrl", "") or ""
    description = lesson.get("description", "") or ""

    print(f"\n{'='*60}")
    print(f"Single-lesson pipeline: {lesson_id}")
    print(f"Course: {course_id}")
    print(f"{'='*60}")

    if _is_vimeo_url(embed_url):
        print("  Detected: Vimeo embed — migrating to YouTube first")
        process_vimeo_lesson(course_id, lesson)
        lesson = _load_lesson(lesson_id, course_id)  # reload: embedUrl now YouTube
        process_lesson(course_id, lesson)
    elif _extract_zoom_info(description) and not embed_url:
        print("  Detected: Zoom recording in description")
        process_zoom_lesson(course_id, lesson)
    else:
        print("  Detected: YouTube (or other transcribable) embed")
        process_lesson(course_id, lesson)


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python run_single_lesson.py <course_id> <lesson_id>")
        sys.exit(1)

    try:
        run_single_lesson(sys.argv[1], sys.argv[2])
    except Exception:
        traceback.print_exc()
        sys.exit(1)
