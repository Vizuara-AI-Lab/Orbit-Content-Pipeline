# Breaking Changes — Miro Multi-Board & Standalone Miro Lessons

## Summary

The pipeline now supports **multiple Miro boards per lesson** and **standalone Miro lessons** (no video, no summary). Two breaking schema changes affect the `Lessons` collection in Orbit Firestore.

---

## Breaking Change 1 — `miroBoardUrl` → `miroBoardUrls`

**Affected collection:** `Lessons`
**Affected lesson type:** `STANDARD`

| Before | After |
|---|---|
| `miroBoardUrl: string \| null` | `miroBoardUrls: string[]` |

A STANDARD lesson that previously had one Miro board now has an array. A lesson with no Miro board previously had `miroBoardUrl: null`; it now has `miroBoardUrls: []`.

**Frontend action required:** Replace every read of `lesson.miroBoardUrl` with `lesson.miroBoardUrls`. Render each URL as a separate iframe tab or panel.

---

## Breaking Change 2 — New lesson type: `MIRO NOTES`

**Affected collection:** `Lessons`

A new lesson type `"MIRO NOTES"` has been added. These are standalone Miro lessons with no lecture video and no generated summary. They appear as curriculum nodes in `topic.lessonIds` alongside STANDARD lessons.

### Document shape

```
{
  id:                            string   // first miroId
  courseId:                      string
  topicId:                       string
  title:                         string
  type:                          "MIRO NOTES"
  description:                   ""
  miroBoardUrls:                 string[] // one or more Miro embed URLs
  durationAddedToLearningProgress: false
  createdAt:                     Timestamp
  updatedAt:                     Timestamp
}
```

Note: `MIRO NOTES` lessons have **no** `videoUrl`, `embedUrl`, `colabUrls`, `shortDescription`, `keyConcepts`, `learningOutcomes`, `chapterMarkers`, `difficulty`, or `estimatedDurationHours` fields. There is also **no** `LessonSummaries` document for them.

**Frontend action required:**
- Handle `type === "MIRO NOTES"` in the lesson renderer — show only the Miro board(s), with no video player or summary tab.
- Do not attempt to fetch a `LessonSummaries` document for these lessons.
- `durationAddedToLearningProgress: false` — do not count these toward course progress duration.

---

## Non-breaking additions

- STANDARD lessons may now have more than one URL in `miroBoardUrls` (previously at most one).
- The `LessonState` pipeline state for a `MIRO NOTES` lesson goes directly to `status: "done"` with no intermediate steps — safe to ignore in progress tracking.
