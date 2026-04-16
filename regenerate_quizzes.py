#!/usr/bin/env python3
"""
Vizuara Orbit — Mandatory Quiz Regeneration Pipeline

Regenerates (overwrites) MandatoryQuizzes documents for every topic in the
given courses, using the latest prod Firestore transcripts and lesson metadata.

Run this when new transcripts are available and you want fresher, fuller quizzes.

Usage:
    python regenerate_quizzes.py [course_id ...]
    python regenerate_quizzes.py --from-json [courses.json]

    If no course IDs are passed, falls back to COURSE_IDS below.

Differences from the main content pipeline quiz step:
  - Always overwrites — no state gating.
  - Full transcripts are used by default. Proportional truncation only kicks in
    when the combined transcript length would exceed the LLM context budget.
  - Question types (multiple_choice / true_false) are interleaved throughout
    the 20-question set rather than batched by type.
"""

import sys
import uuid
import traceback

from run_content_pipeline import (
    prod_db,
    _fetch_all_lessons,
    _get_video_url,
    _extract_zoom_info,
    write_quiz,
)
from run_pipeline import (
    _llm_json_array,
)

# ─── Course IDs to process ────────────────────────────────────────────────────
COURSE_IDS: list[str] = [
    # "course_20006089",
]

# ─── Context budget ───────────────────────────────────────────────────────────
# gpt-4o has a 128k-token context window.
# We reserve ~12k tokens for system prompt, headers, key concepts, and the
# 8192-token output budget.  The remainder (~116k tokens ≈ 464k chars at
# ~4 chars/token) is available for transcript text.  We use 400k chars as a
# conservative threshold so we stay well clear of the hard limit.
QUIZ_CONTEXT_BUDGET = 400_000  # chars

# ─── System prompt ────────────────────────────────────────────────────────────
QUIZ_SYSTEM = """
You are an expert educator creating quiz questions for an online learning platform.
Given transcripts and key concepts from all lessons in a course topic, generate exactly 20 questions.

Rules:
- Draw from ALL lessons provided (distribute questions proportionally across lessons).
- Mix question types: 10 multiple_choice and 10 true_false.
- IMPORTANT: Interleave the two types throughout the list — do NOT place all
  multiple_choice questions first and all true_false questions last (or vice versa).
  The sequence should alternate or otherwise feel mixed, e.g. MC, TF, MC, TF, …
  or similar irregular but balanced patterns. Never group all of one type together.
- multiple_choice distractors must be plausible — not obviously wrong.
- Tag each question to its primary concept and lesson.

Return ONLY a valid JSON array of exactly 20 objects:
[{
  "id": "q_<8-char hex>",
  "type": "multiple_choice" | "true_false",
  "text": "question text",
  "options": ["A","B","C","D"] | null,
  "correctIndex": 0|1|2|3 | null,
  "conceptTags": ["concept"],
  "lessonId": "lesson_id"
}]

For true_false questions, set options to ["True", "False"] and correctIndex to 0 (True) or 1 (False).
For multiple_choice questions, options must have exactly 4 items and correctIndex must be 0–3.
"""


def _build_quiz_context(course_id: str, topic_id: str, lesson_ids: list[str]) -> str:
    """
    Assemble the user-turn text for quiz generation.

    Fetches full transcripts from prod.  If the total character count of all
    transcripts exceeds QUIZ_CONTEXT_BUDGET, each transcript is trimmed
    proportionally to fit within the budget.
    """
    # Gather raw data first (full transcripts, no truncation yet)
    lessons_data = []
    for i, lid in enumerate(lesson_ids):
        t = prod_db.collection("Transcripts").document(f"{course_id}_{lid}").get().to_dict() or {}
        l = prod_db.collection("Lessons").document(lid).get().to_dict() or {}
        full_text = t.get("fullText", "")
        concepts  = ", ".join(l.get("keyConcepts", []))
        title     = l.get("title", lid)
        lessons_data.append({
            "index":    i + 1,
            "lid":      lid,
            "title":    title,
            "concepts": concepts,
            "text":     full_text,
        })

    # Decide whether truncation is needed
    total_chars = sum(len(d["text"]) for d in lessons_data)
    if total_chars > QUIZ_CONTEXT_BUDGET and total_chars > 0:
        # Distribute budget proportionally by each lesson's share of the total
        for d in lessons_data:
            share     = len(d["text"]) / total_chars
            allowance = int(QUIZ_CONTEXT_BUDGET * share)
            d["text"] = d["text"][:allowance]
        print(
            f"    [QUIZ] Transcripts truncated: {total_chars:,} → "
            f"{sum(len(d['text']) for d in lessons_data):,} chars"
        )
    else:
        print(f"    [QUIZ] Using full transcripts ({total_chars:,} chars total)")

    # Build the prompt text
    parts = []
    for d in lessons_data:
        parts.append(
            f"--- Lesson {d['index']}: {d['title']} (ID: {d['lid']}) ---\n"
            f"Key concepts: {d['concepts']}\n\nTranscript:\n{d['text']}"
        )

    return f"Topic ID: {topic_id} — {len(lesson_ids)} lesson(s)\n\n" + "\n\n".join(parts)


def generate_quiz(course_id: str, topic_id: str, lesson_ids: list[str]) -> dict:
    """Generate a fresh mandatory quiz for a topic using prod transcripts."""
    user      = _build_quiz_context(course_id, topic_id, lesson_ids)
    questions = _llm_json_array(QUIZ_SYSTEM, user, max_tokens=8192)

    for q in questions:
        if not q.get("id"):
            q["id"] = f"q_{uuid.uuid4().hex[:8]}"

    return {"topicId": topic_id, "sourceLessonIds": lesson_ids, "questions": questions}


# ══════════════════════════════════════════════════════════════════════════════
# COURSE ORCHESTRATOR
# ══════════════════════════════════════════════════════════════════════════════

def run_course(course_id: str):
    print(f"\n{'='*60}")
    print(f"Quiz regeneration: {course_id}")
    print(f"{'='*60}")

    all_lessons = _fetch_all_lessons(course_id)
    if not all_lessons:
        print(f"  [WARN] No lessons found for course {course_id}")
        return

    # Build topic → lesson_ids mapping (same subset the main pipeline uses)
    transcribable = [l for l in all_lessons if _get_video_url(l)]
    zoom_lessons  = [l for l in all_lessons if not _get_video_url(l) and _extract_zoom_info(l.get("description", ""))]

    topics: dict[str, list[str]] = {}
    for lesson in transcribable + zoom_lessons:
        tid = lesson["topicId"]
        topics.setdefault(tid, []).append(lesson["id"])

    if not topics:
        print("  [WARN] No transcribable lessons found — nothing to quiz.")
        return

    print(f"  Found {len(topics)} topic(s) with transcribable lessons.")

    for topic_id, lesson_ids in topics.items():
        print(f"\n  Topic {topic_id} ({len(lesson_ids)} lesson(s))")
        try:
            quiz = generate_quiz(course_id, topic_id, lesson_ids)
            write_quiz(course_id, topic_id, quiz)
            print(f"  ✓ Quiz written ({len(quiz['questions'])} questions)")
        except Exception:
            print(f"  [ERROR] Quiz for topic {topic_id} failed:")
            traceback.print_exc()


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
        print(f"Loaded {len(ids)} course ID(s) from {json_path}")
    else:
        ids = args or COURSE_IDS

    if not ids:
        print("No course IDs provided. Add them to COURSE_IDS or pass as arguments.")
        sys.exit(1)

    for cid in ids:
        run_course(cid)
