"""
Orbit Content Pipeline — Firestore Document Schemas
====================================================
Reference file. Not imported by the pipeline.
Describes every document shape written to the Orbit Firestore.

Collections
-----------
Lessons/{lessonId}
Quizzes/mandatory_{topicId}
Transcripts/{courseId}_{lessonId}
LessonSummaries/{courseId}_{lessonId}
Courses/{courseId}                          ← pipeline merges fields only
_PipelineState/{courseId}
_PipelineState/{courseId}/Lessons/{lessonId}
"""

from typing import TypedDict, Literal, Optional


# ── Shared primitives ─────────────────────────────────────────────────────────

class Duration(TypedDict):
    hours: int
    minutes: int


class ChapterMarker(TypedDict):
    timestamp: float    # seconds from start
    label: str


class OrbitTopic(TypedDict):
    id: str
    title: str
    lessonIds: list[str]    # ordered; always ends with a "quiz_{topicId}" entry


# ── Lessons/{lessonId} ────────────────────────────────────────────────────────
# Written by write_lesson_to_orbit() for STANDARD lessons.

class LessonStandard(TypedDict):
    id: str
    courseId: str
    topicId: str
    title: str
    type: Literal["STANDARD"]
    description: str
    # Content URLs
    videoUrl: Optional[str]
    embedUrl: Optional[str]
    miroBoardUrl: Optional[str]     # Miro board, rendered in MIRO NOTES tab via iframe
    colabUrls: list[str]            # plain links, each opens in new tab
    # Copied from production DB
    duration: Duration
    # Pipeline-generated
    shortDescription: str
    keyConcepts: list[str]
    learningOutcomes: list[str]
    chapterMarkers: list[ChapterMarker]
    difficulty: Literal["beginner", "intermediate", "advanced"]
    estimatedDurationHours: float
    durationAddedToLearningProgress: Literal[True]
    createdAt: object   # SERVER_TIMESTAMP
    updatedAt: object   # SERVER_TIMESTAMP


# Written by write_quiz() as the curriculum sidebar node for the quiz.

class LessonMandatoryQuiz(TypedDict):
    id: str                             # "quiz_{topicId}"
    courseId: str
    topicId: str
    title: str                          # "Topic Quiz"
    description: str                    # ""
    type: Literal["MANDATORY QUIZ"]
    duration: Duration                  # {"hours": 0, "minutes": 0}
    durationAddedToLearningProgress: Literal[False]
    createdAt: object   # SERVER_TIMESTAMP
    updatedAt: object   # SERVER_TIMESTAMP


# ── Quizzes/mandatory_{topicId} ───────────────────────────────────────────────
# Written by write_quiz().

class QuizQuestion(TypedDict):
    id: str                             # "q_<8-char hex>"
    type: Literal["multiple_choice", "true_false"]
    text: str
    options: Optional[list[str]]        # ["A","B","C","D"] or ["True","False"]
    correctIndex: Optional[int]         # 0-based
    conceptTags: list[str]
    lessonId: str


class Quiz(TypedDict):
    id: str                             # "mandatory_{topicId}"
    courseId: str
    topicId: str
    sourceLessonIds: list[str]
    questions: list[QuizQuestion]       # always 20: 10 multiple_choice + 10 true_false
    createdAt: object   # SERVER_TIMESTAMP


# ── Transcripts/{courseId}_{lessonId} ─────────────────────────────────────────
# Written by write_transcript().

class TranscriptSegment(TypedDict):
    start: float
    end: float
    text: str


class Transcript(TypedDict):
    courseId: str
    lessonId: str
    segments: list[TranscriptSegment]
    fullText: str
    createdAt: object   # SERVER_TIMESTAMP


# ── LessonSummaries/{courseId}_{lessonId} ─────────────────────────────────────
# Written by write_summary() after figure generation (step 4).

class LessonSummary(TypedDict):
    courseId: str
    lessonId: str
    content: str        # final markdown with figure image URLs substituted in
    contentRaw: str     # original markdown with [FIGURE: "..."] placeholders
    figureCount: int
    createdAt: object   # SERVER_TIMESTAMP


# ── Courses/{courseId} ────────────────────────────────────────────────────────
# seed_course() copies base fields from prod at the start of each run (merge=True).
# aggregate_course() / write_course() then merges pipeline-generated fields on top.

class CourseSeededFields(TypedDict):
    """Copied verbatim from the production course document by seed_course()."""
    id: str
    title: str
    slug: str
    description: str
    duration: object            # Duration
    thumbnail: Optional[str]
    regularPrice: float
    salePrice: float
    pricingModel: str           # PricingModel enum value
    subscriptionPlans: Optional[list]
    categoryIds: list[str]
    targetAudienceIds: list[str]
    tags: list[str]
    instructorId: str
    instructorName: str
    status: str                 # CourseStatus enum value
    mode: str                   # CourseMode enum value
    liveAt: Optional[object]    # Timestamp | null
    certificateTemplateId: Optional[str]
    isEnrollmentPaused: bool
    isMailSendingEnabled: bool
    isCertificateEnabled: bool
    isCourseCompletionEnabled: bool
    customCertificateName: str
    isForumEnabled: bool
    isWelcomeMessageEnabled: bool
    externalToolLink: Optional[str]
    createdAt: object           # Timestamp


class CoursePipelineFields(TypedDict):
    """Merged on top of CourseSeededFields by write_course() at end of pipeline run."""
    topics: list[OrbitTopic]            # replaces the production Topic[] array
    shortDescription: str
    prerequisites: list[str]            # controlled vocabulary strings
    difficulty: Literal["beginner", "intermediate", "advanced"]
    topicsCovered: list[str]            # deduplicated concept tags, max 30
    estimatedDurationHours: float       # sum of all STANDARD lesson durations
    updatedAt: object   # SERVER_TIMESTAMP


# ── _PipelineState/{courseId} ─────────────────────────────────────────────────
# Written by set_course_state(). Tracks overall course run progress.
# Uses merge=True — only updated keys are written.

class CourseState(TypedDict, total=False):
    status: Literal["processing", "done", "failed"]
    failedAt: str                       # lesson_id of the first failure
    # Per-topic quiz completion flags, keyed as "quiz_topic_{topicId}"
    # e.g. quiz_topic_topic_abc123: Literal["done"]
    updatedAt: object   # SERVER_TIMESTAMP


# ── _PipelineState/{courseId}/Lessons/{lessonId} ─────────────────────────────
# Written by set_lesson_state(). Tracks per-lesson step completion.
# Uses merge=True — only updated keys are written.

class LessonState(TypedDict, total=False):
    status: Literal["transcribing", "extracting", "summarizing", "figures", "done", "failed"]
    stepsCompleted: list[Literal["transcription", "extraction", "summary", "figures"]]
    error: str                          # set on failure
    updatedAt: object   # SERVER_TIMESTAMP
