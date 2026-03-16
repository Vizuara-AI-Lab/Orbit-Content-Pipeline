# Lesson Summary Generation — Format Specification

**Owner:** Generation pipeline team
**Status:** Migration in progress — switching from Markdown to LaTeX
**Effective from:** Next generation run

---

## What is changing

Lesson summaries are currently written to Firestore as **Markdown**. They will now be written as **LaTeX body content**. The PDF compilation step has moved server-side (Cloud Run + pdflatex), so the format the app receives must be LaTeX.

---

## Firestore target

| Field | Collection | Document ID format |
|---|---|---|
| `content` | `LessonSummaries` | `{courseId}_{lessonId}` |

The full document schema:

```
LessonSummaries / {courseId}_{lessonId}
  ├── courseId:    string   — e.g. "phys101"
  ├── lessonId:    string   — e.g. "lesson_03"
  ├── content:     string   — LaTeX body (see below) ← THIS FIELD IS CHANGING
  ├── contentRaw:  string   — LaTeX body with [FIGURE: "desc"] placeholders still in place
  ├── figureCount: number   — how many figures were generated
  └── format:      string   — set to "latex" (NEW FIELD — add this)
```

The new `format: "latex"` field tells the app which rendering path to use. **Always write it.** Documents without this field will be treated as legacy Markdown.

---

## What `content` must contain

`content` must be a **LaTeX document body** — not a full document, just the content that goes between `\begin{document}` and `\end{document}`. The app wraps it in a template automatically.

### Rules

1. **No preamble.** Do not include `\documentclass`, `\usepackage`, `\begin{document}`, or `\end{document}`. Body content only.

2. **Use standard LaTeX sectioning.**
   ```latex
   \section{Introduction}
   \subsection{Key Concepts}
   ```

3. **Images use `\includegraphics` with the full Firebase Storage URL as the filename.**
   ```latex
   \begin{figure}[H]
     \centering
     \includegraphics[width=0.85\textwidth]{https://firebasestorage.googleapis.com/v0/b/orbit-fdbc9.firebasestorage.app/o/...?alt=media}
     \caption{Diagram showing Newton's second law}
   \end{figure}
   ```
   The compiler fetches images by URL at build time. Use the `firebasestorage.googleapis.com` URL format (not the `storage.googleapis.com` format).

4. **Math is supported.** Use standard LaTeX math environments:
   ```latex
   Inline: $F = ma$
   Display: \[ E = mc^2 \]
   Equation block:
   \begin{equation}
     \nabla \cdot \vec{E} = \frac{\rho}{\varepsilon_0}
   \end{equation}
   ```

5. **No custom packages.** Only use commands available in the standard template packages:
   - `graphicx` — images
   - `amsmath`, `amssymb` — math
   - `hyperref` — links
   - `booktabs` — tables
   - `enumitem` — lists
   - `float` — `[H]` figure placement

6. **Special characters must be escaped.**

   | Character | Escaped form |
   |---|---|
   | `&` | `\&` |
   | `%` | `\%` |
   | `$` (outside math) | `\$` |
   | `#` | `\#` |
   | `_` (outside math) | `\_` |
   | `{` `}` (outside commands) | `\{` `\}` |
   | `~` | `\textasciitilde{}` |
   | `^` (outside math) | `\textasciicircum{}` |

---

## `contentRaw` — what it must contain

Same as `content`, but with `[FIGURE: "description"]` placeholders instead of resolved `\includegraphics` blocks. This is kept as the source of truth for re-runs.

```latex
% contentRaw example:
\section{Projectile Motion}

A projectile follows a parabolic path under gravity.

[FIGURE: "Diagram of projectile trajectory showing horizontal and vertical components"]

The horizontal velocity remains constant while vertical velocity changes due to gravity.
```

After figure generation, `[FIGURE: "description"]` is replaced with the full `\begin{figure}...\end{figure}` block in `content`.

---

## Complete example

### `contentRaw`
```latex
\section{Newton's Laws of Motion}

Newton's three laws describe the relationship between a body and the forces acting upon it.

\subsection{First Law — Inertia}

An object at rest stays at rest, and an object in motion stays in motion, unless acted upon by a net external force.

\subsection{Second Law — $F = ma$}

The net force on an object equals its mass multiplied by its acceleration:
\begin{equation}
  \vec{F} = m\vec{a}
\end{equation}

[FIGURE: "Free body diagram showing forces acting on a block on an inclined plane"]

\subsection{Third Law — Action and Reaction}

For every action there is an equal and opposite reaction. If object A exerts force $\vec{F}$ on object B, then B exerts $-\vec{F}$ on A.
```

### `content` (after figure substitution)
```latex
\section{Newton's Laws of Motion}

Newton's three laws describe the relationship between a body and the forces acting upon it.

\subsection{First Law — Inertia}

An object at rest stays at rest, and an object in motion stays in motion, unless acted upon by a net external force.

\subsection{Second Law — $F = ma$}

The net force on an object equals its mass multiplied by its acceleration:
\begin{equation}
  \vec{F} = m\vec{a}
\end{equation}

\begin{figure}[H]
  \centering
  \includegraphics[width=0.85\textwidth]{https://firebasestorage.googleapis.com/v0/b/orbit-fdbc9.firebasestorage.app/o/figures%2Fphys101_lesson_03_fig1.png?alt=media}
  \caption{Free body diagram showing forces acting on a block on an inclined plane}
\end{figure}

\subsection{Third Law — Action and Reaction}

For every action there is an equal and opposite reaction. If object A exerts force $\vec{F}$ on object B, then B exerts $-\vec{F}$ on A.
```

---

## Prompt guidance

When prompting the AI to generate a summary, instruct it to:

- Output **LaTeX body content only** (no preamble, no `\begin{document}`)
- Use `\section` and `\subsection` for structure
- Use `[FIGURE: "concise description"]` as a placeholder wherever a diagram would aid understanding — the figure pipeline will generate and replace it
- Use proper LaTeX math environments for all equations
- Escape all special characters
- Keep figure captions descriptive (they appear in the PDF below the image)
- Do **not** use any packages not listed above

---

## Legacy documents

Documents written before this migration have no `format` field (or `format: "markdown"`). The app will continue to render them using the old Markdown path. There is no need to immediately backfill — they will be updated on the next re-generation run for each lesson.
