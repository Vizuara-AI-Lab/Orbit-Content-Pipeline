#!/usr/bin/env python3
"""
Vizuara Orbit — Summary Regeneration Pipeline

Regenerates the summary and figures for a single lesson from its existing
transcript. The lesson must already have a transcript in Firestore.

Steps:
  1. Load transcript from Transcripts/{courseId}_{lessonId}
  2. Load lesson metadata (title, keyConcepts, learningOutcomes, chapterMarkers)
  3. generate_summary()  → LaTeX + Markdown
  4. generate_figures()  → uploads figures to prod storage, writes to LessonSummaries

Usage:
    python run_regen_summary.py <course_id> <lesson_id>
"""

import sys
import traceback

# Importing run_content_pipeline patches run_pipeline._paperbanana_and_upload
# with the key-rotating fallback version automatically (line 124 of that module).
import run_pipeline
import run_content_pipeline  # noqa: F401 — side-effect import (patches run_pipeline)
from run_pipeline import prod_db, generate_summary, generate_figures
from run_content_pipeline import write_summary


def regen_summary(course_id: str, lesson_id: str):
    # Route figure uploads to prod storage, same as run_content_pipeline does.
    run_pipeline.orbit_bucket = run_pipeline.prod_bucket

    print(f"\n{'='*60}")
    print(f"Regenerating summary: {lesson_id}")
    print(f"Course: {course_id}")
    print(f"{'='*60}")

    # ── Load transcript ───────────────────────────────────────────────────────
    print("  [1/3] Loading transcript...")
    transcript_doc = prod_db.collection("Transcripts").document(f"{course_id}_{lesson_id}").get()
    if not transcript_doc.exists:
        raise ValueError(
            f"No transcript found for Transcripts/{course_id}_{lesson_id}. "
            "Run the full pipeline first."
        )
    transcript = transcript_doc.to_dict()

    # ── Load lesson metadata ──────────────────────────────────────────────────
    lesson_doc = prod_db.collection("Lessons").document(lesson_id).get()
    if not lesson_doc.exists:
        raise ValueError(f"Lesson {lesson_id} not found in Firestore.")
    lesson = lesson_doc.to_dict()
    title = lesson.get("title", lesson_id)

    # ── Generate summary ──────────────────────────────────────────────────────
    print("  [2/3] Generating summary...")
    summary = generate_summary(transcript, lesson, title)
    print(f"        {summary['figureCount']} figure(s) to generate")

    # ── Generate figures ──────────────────────────────────────────────────────
    print("  [3/3] Generating figures...")
    final_summary = generate_figures(course_id, lesson_id, summary)
    write_summary(course_id, lesson_id, final_summary)

    print(f"\n  ✓ Summary written to LessonSummaries/{course_id}_{lesson_id}")


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python run_regen_summary.py <course_id> <lesson_id>")
        sys.exit(1)

    course_id = sys.argv[1]
    lesson_id = sys.argv[2]

    try:
        regen_summary(course_id, lesson_id)
    except Exception:
        traceback.print_exc()
        sys.exit(1)
