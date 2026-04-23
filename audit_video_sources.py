#!/usr/bin/env python3
"""
Audit lessons across the courses listed in courses.json. For each course,
count lessons that the content pipeline would actually route to:
  - vimeo:N — lessons whose resolved video URL (_get_video_url) is a Vimeo link
  - zoom:N  — lessons with no video URL at all but with a Zoom recording
              URL + passcode in the description

These are exactly the filters run_content_pipeline.py applies when splitting
lessons into its `transcribable` and `zoom_lessons` buckets — so the counts
reflect what the content pipeline would pick up, not raw URL mentions.

Prints one line per course that has at least one hit:
    course_id | Course Title | zoom:N vimeo:M

Usage:
    python audit_video_sources.py [courses.json]
"""

import json
import sys

from run_pipeline import prod_db
from run_content_pipeline import _get_video_url, _extract_zoom_info


def _already_uploaded(course_id: str, lesson_id: str) -> bool:
    """
    True if the Zoom/Vimeo pipeline has already uploaded this lesson to YouTube.
    The "youtube_upload" step marker is written by both pipelines after a
    successful upload — used here to skip lessons whose description still
    carries the original Zoom URL + passcode even though processing is done.
    """
    doc = (
        prod_db.collection("_PipelineStateProd")
        .document(course_id)
        .collection("Lessons")
        .document(lesson_id)
        .get()
    )
    if not doc.exists:
        return False
    return "youtube_upload" in (doc.to_dict() or {}).get("stepsCompleted", [])


def _classify(lesson: dict) -> str | None:
    """Return 'vimeo', 'zoom', or None — mirroring the content pipeline's routing."""
    video_url = _get_video_url(lesson)
    if video_url:
        if "vimeo.com" in video_url.lower():
            return "vimeo"
        return None  # YouTube (or other transcribable) — not counted
    if _extract_zoom_info(lesson.get("description", "")):
        return "zoom"
    return None


def audit_course(course_id: str) -> tuple[str, int, int]:
    """Return (title, zoom_count, vimeo_count) for a single course."""
    course_doc = prod_db.collection("Courses").document(course_id).get()
    if not course_doc.exists:
        return (f"<{course_id} not found>", 0, 0)
    course = course_doc.to_dict() or {}
    title = course.get("title", "")
    topics = course.get("topics", [])

    lesson_ids = [
        item["id"]
        for topic in topics
        for item in topic.get("items", [])
        if not (item.get("id") or "").startswith("quiz_")
    ]

    zoom = vimeo = 0
    for lid in lesson_ids:
        doc = prod_db.collection("Lessons").document(lid).get()
        if not doc.exists:
            continue
        kind = _classify(doc.to_dict() or {})
        if kind is None:
            continue
        if _already_uploaded(course_id, lid):
            continue
        if kind == "zoom":
            zoom += 1
        elif kind == "vimeo":
            vimeo += 1

    return (title, zoom, vimeo)


def main():
    path = sys.argv[1] if len(sys.argv) > 1 else "courses.json"
    with open(path) as f:
        course_ids = [c["firestoreId"] for c in json.load(f) if c.get("firestoreId")]

    print(f"Auditing {len(course_ids)} course(s) from {path}\n")

    for cid in course_ids:
        title, zoom, vimeo = audit_course(cid)
        if zoom == 0 and vimeo == 0:
            continue
        parts = []
        if zoom:
            parts.append(f"zoom:{zoom}")
        if vimeo:
            parts.append(f"vimeo:{vimeo}")
        print(f"{cid} | {title} | {' '.join(parts)}")


if __name__ == "__main__":
    main()
