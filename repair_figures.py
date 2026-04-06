#!/usr/bin/env python3
"""
repair_figures.py — Re-generates unresolved figures in LessonSummaries.

Scans every LessonSummaries document for leftover [FIGURE: ...] placeholders
in `content` (i.e. blocks the original pipeline never replaced because the LLM
omitted the required quotes), runs PaperBanana for each, and patches
content / contentRaw in-place.

Also handles the older <!-- figure failed: ... --> comment style for
documents processed before this fix.

Usage:
    python repair_figures.py                   # scan all LessonSummaries
    python repair_figures.py courseA_lessonB   # target specific doc IDs
"""

import re
import sys

from run_pipeline import (
    orbit_db,
    _enrich_figure_description,
    _paperbanana_and_upload,
)

# Matches [FIGURE: "desc"] or [FIGURE: desc] — with or without quotes
_ANY_FIGURE_RE = re.compile(r'\[FIGURE:\s*"?([^"\]\n]+?)"?\s*\]')

# Older failure comment style left by the pipeline
_FAILED_MD_RE    = re.compile(r'<!-- figure failed: (.*?) -->')
_FAILED_LATEX_RE = re.compile(r'% figure failed: (.{1,80})')


def _surrounding(text: str, match: re.Match) -> str:
    """±600 chars around a regex match, excluding the match itself."""
    start = max(0, match.start() - 600)
    end   = min(len(text), match.end() + 600)
    return text[start:match.start()] + text[match.end():end]


def _desc_index_map(raw_md: str) -> dict[str, int]:
    """
    Build description → storage index from contentMarkdownRaw.
    Uses first-appearance order of unique descriptions (same logic as
    generate_figures in run_pipeline.py).
    """
    seen: list[str] = []
    for m in _ANY_FIGURE_RE.finditer(raw_md):
        desc = m.group(1).strip()
        if desc not in seen:
            seen.append(desc)
    return {d: i for i, d in enumerate(seen)}


def _find_full_desc(short: str, raw_md: str) -> str | None:
    """
    Resolve a truncated description (from a failed comment) back to the
    full description in contentMarkdownRaw. Returns None if not found.
    """
    for m in _ANY_FIGURE_RE.finditer(raw_md):
        desc = m.group(1).strip()
        if desc[:80] == short.strip():
            return desc
    return None


def repair_document(doc_id: str, data: dict) -> bool:
    """
    Repairs all unresolved figures in one LessonSummaries document.
    Returns True if the document was updated.
    """
    content     = data.get("content", "")
    content_raw = data.get("contentRaw", "")
    raw_md      = data.get("contentMarkdownRaw", "")
    course_id   = data.get("courseId", "")
    lesson_id   = data.get("lessonId", "")

    has_unresolved = bool(_ANY_FIGURE_RE.search(content))
    has_failed_comment = "<!-- figure failed:" in content
    if not has_unresolved and not has_failed_comment:
        return False

    if not raw_md:
        print(f"  [SKIP] {doc_id} — contentMarkdownRaw missing, cannot determine indices")
        return False

    desc_to_index = _desc_index_map(raw_md)

    # --- Collect all descriptions that need generation ---

    # 1. Unresolved [FIGURE: ...] blocks still sitting in content
    unresolved_descs: list[str] = []
    for m in _ANY_FIGURE_RE.finditer(content):
        desc = m.group(1).strip()
        if desc not in unresolved_descs:
            unresolved_descs.append(desc)

    # 2. Old <!-- figure failed: ... --> comments
    for m in _FAILED_MD_RE.finditer(content):
        full = _find_full_desc(m.group(1), raw_md)
        if full and full not in unresolved_descs:
            unresolved_descs.append(full)
        elif not full:
            print(f"  [WARN] Cannot resolve failed comment: {m.group(1)!r}")

    if not unresolved_descs:
        return False

    print(f"  {len(unresolved_descs)} unresolved figure(s) in {doc_id}")

    # --- Generate and upload ---
    desc_to_url: dict[str, str] = {}
    for desc in unresolved_descs:
        index = desc_to_index.get(desc)
        if index is None:
            # Description not in raw_md (e.g. typo drift) — assign next available index
            index = max(desc_to_index.values(), default=-1) + 1
            desc_to_index[desc] = index
            print(f"    [WARN] Description not in raw_md, using index {index}: {desc[:60]!r}")

        # Grab surrounding context from contentMarkdownRaw for enrichment
        raw_match = next(
            (m for m in _ANY_FIGURE_RE.finditer(raw_md) if m.group(1).strip() == desc),
            None,
        )
        surrounding = _surrounding(raw_md, raw_match) if raw_match else ""

        print(f"    [{index}] {desc[:70]!r}...")
        try:
            enriched = _enrich_figure_description(desc, surrounding)
            url = _paperbanana_and_upload(enriched, course_id, lesson_id, index)
            desc_to_url[desc] = url
            print(f"          OK → {url}")
        except Exception as e:
            print(f"          [FAIL] {e}")

    if not desc_to_url:
        return False

    # --- Patch content (Markdown) ---

    def patch_unresolved(m: re.Match) -> str:
        desc = m.group(1).strip()
        if desc in desc_to_url:
            return f"![{desc[:60]}]({desc_to_url[desc]})"
        return m.group(0)

    def patch_failed_comment(m: re.Match) -> str:
        full = _find_full_desc(m.group(1), raw_md)
        if full and full in desc_to_url:
            return f"![{full[:60]}]({desc_to_url[full]})"
        return m.group(0)

    new_content = _ANY_FIGURE_RE.sub(patch_unresolved, content)
    new_content = _FAILED_MD_RE.sub(patch_failed_comment, new_content)

    # --- Patch contentRaw (LaTeX) ---

    def patch_latex_unresolved(m: re.Match) -> str:
        desc = m.group(1).strip()
        if desc in desc_to_url:
            return _latex_figure(desc, desc_to_url[desc])
        return m.group(0)

    def patch_latex_failed(m: re.Match) -> str:
        full = _find_full_desc(m.group(1), raw_md)
        if full and full in desc_to_url:
            return _latex_figure(full, desc_to_url[full])
        return m.group(0)

    new_content_raw = _ANY_FIGURE_RE.sub(patch_latex_unresolved, content_raw)
    new_content_raw = _FAILED_LATEX_RE.sub(patch_latex_failed, new_content_raw)

    still_unresolved = len(_ANY_FIGURE_RE.findall(new_content))
    new_figure_count = data.get("figureCount", 0) + len(desc_to_url)

    orbit_db.collection("LessonSummaries").document(doc_id).update({
        "content":     new_content,
        "contentRaw":  new_content_raw,
        "figureCount": new_figure_count,
    })

    print(f"  Saved {doc_id}: +{len(desc_to_url)} fixed, {still_unresolved} still unresolved\n")
    return True


def _latex_figure(desc: str, url: str) -> str:
    caption = desc[:120].replace("{", r"\{").replace("}", r"\}")
    return (
        f"\\begin{{figure}}[H]\n"
        f"\\centering\n"
        f"\\includegraphics[width=\\linewidth]{{{url}}}\n"
        f"\\caption{{{caption}}}\n"
        f"\\end{{figure}}"
    )


def main():
    if len(sys.argv) > 1:
        doc_ids  = sys.argv[1:]
        raw_docs = [orbit_db.collection("LessonSummaries").document(d).get() for d in doc_ids]
        items    = [(d.id, d.to_dict()) for d in raw_docs if d.exists]
        missing  = [doc_ids[i] for i, d in enumerate(raw_docs) if not d.exists]
        if missing:
            print(f"[WARN] Not found in LessonSummaries: {missing}")
    else:
        print("Scanning all LessonSummaries for unresolved [FIGURE:] blocks...\n")
        items = [
            (d.id, d.to_dict())
            for d in orbit_db.collection("LessonSummaries").stream()
            if "[FIGURE:" in (d.to_dict() or {}).get("content", "")
            or "<!-- figure failed:" in (d.to_dict() or {}).get("content", "")
        ]

    if not items:
        print("No documents with unresolved figures found.")
        return

    print(f"Found {len(items)} document(s) to repair\n")

    updated = 0
    for doc_id, data in items:
        print(f"Processing {doc_id}...")
        if repair_document(doc_id, data):
            updated += 1

    print(f"Done. {updated}/{len(items)} document(s) updated.")


if __name__ == "__main__":
    main()
