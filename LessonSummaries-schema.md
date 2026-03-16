# LessonSummaries — Firestore Schema

Collection: `LessonSummaries`
Document ID: `{courseId}_{lessonId}`

---

## Fields

| Field | Type | Description |
|---|---|---|
| `courseId` | string | Parent course ID |
| `lessonId` | string | Lesson ID |
| `content` | string | Lesson summary in **Markdown**, with figures fully resolved as image tags |
| `contentRaw` | string | Lesson summary in **LaTeX**, with figures fully resolved as `\begin{figure}[H]` blocks |
| `figureCount` | int | Number of successfully generated figures |
| `createdAt` | timestamp | Firestore server timestamp |

---

## Image format in `content` (Markdown)

Each figure placeholder is replaced with a standard Markdown image tag:

```markdown
![Diagram showing self-attention mechanism focusing on a single word](https://storage.googleapis.com/...)
```

The alt text is the first 60 characters of the original figure description.
The URL points to a PNG hosted on Firebase Storage.

### Frontend rendering

Render `content` with any standard Markdown renderer (e.g. `react-markdown`, `marked`).
Images will render automatically via the `<img>` tags the renderer produces from the `![](url)` syntax.
No special handling needed.

---

## Image format in `contentRaw` (LaTeX)

Each figure placeholder is replaced with a `figure` float environment pinned in place with `[H]`:

```latex
\begin{figure}[H]
\centering
\includegraphics[width=\linewidth]{https://storage.googleapis.com/...}
\caption{Diagram showing self-attention mechanism focusing on a single word's relationship to the entire sequence}
\end{figure}
```

The `[H]` specifier (from the `float` package) forces the figure to appear exactly at the point in the text where the placeholder was — it does not float to the top or bottom of the page. This prevents images from being cut across pages or repositioned when generating PDFs.

### Frontend rendering / PDF generation

- The LaTeX preamble **must** include `\usepackage{float}` for `[H]` to be recognised.
- `\includegraphics` fetches the image from the Firebase Storage URL directly. The LaTeX compiler or rendering engine must have network access, or the images must be downloaded and paths substituted before compilation.
- `width=\linewidth` scales each image to the full text width, which is appropriate for educational diagrams that need to be legible.

---

## Failed figures

If figure generation fails for a placeholder:

- In `content` (Markdown): `<!-- figure failed: {description} -->`
- In `contentRaw` (LaTeX): `% figure failed: {description}`

Both are invisible to readers (HTML comment and LaTeX comment respectively) but preserve the original description for debugging.
