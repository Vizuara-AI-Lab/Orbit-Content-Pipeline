#!/usr/bin/env python3
"""
Audit lessons across the courses listed in courses.json. For each course,
count lessons that contain:
  - a Vimeo URL (in embedUrl or description)
  - a Zoom recording URL WITH passcode (in embedUrl or description)

Prints one line per course that has at least one hit:
    course_id | Course Title | zoom:N vimeo:M

Usage:
    python audit_video_sources.py [courses.json]
"""

import json
import re
import sys

from run_pipeline import prod_db
from run_content_pipeline import _extract_zoom_info


_VIMEO_RE = re.compile(r'vimeo\.com/', re.IGNORECASE)


def _has_vimeo(lesson: dict) -> bool:
    for field in ("embedUrl", "description"):
        if _VIMEO_RE.search(lesson.get(field) or ""):
            return True
    return False


def _has_zoom_with_passcode(lesson: dict) -> bool:
    # _extract_zoom_info requires both a zoom.us/rec/ URL and a "Passcode: …"
    # line; it returns None otherwise. Check both fields.
    for field in ("embedUrl", "description"):
        if _extract_zoom_info(lesson.get(field) or ""):
            return True
    return False


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
        lesson = doc.to_dict() or {}
        if _has_zoom_with_passcode(lesson):
            zoom += 1
        if _has_vimeo(lesson):
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
