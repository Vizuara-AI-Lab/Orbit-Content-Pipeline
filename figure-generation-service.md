# Figure Generation — Cloud Run Service Spec

## Overview

A standalone HTTP microservice that accepts a figure description, runs the PaperBanana
multi-agent pipeline to generate a teaching-grade diagram, uploads the result PNG to
Firebase Storage, and returns the public image URL.

Built to serve Vizuara's Orbit platform. Intended to be deployed as a separate Cloud Run
Service with no dependency on the content pipeline codebase.

---

## Endpoint

```
POST /generate-figure
Content-Type: application/json
```

### Request body

```json
{
  "description": "A diagram showing how backpropagation flows gradients through a neural network",
  "courseId": "course_abc123",
  "lessonId": "lesson_xyz456",
  "index": 0
}
```

| Field | Type | Required | Description |
|---|---|---|---|
| `description` | string | yes | Plain-text description of the diagram to generate |
| `courseId` | string | yes | Used to construct the Firebase Storage path |
| `lessonId` | string | yes | Used to construct the Firebase Storage path |
| `index` | integer | yes | Zero-based figure index within the lesson (0, 1, 2 …) |

### Response — success (`200`)

```json
{
  "url": "https://storage.googleapis.com/orbit-bucket/lesson_figures/course_abc123/lesson_xyz456/figure_000.png"
}
```

### Response — error (`4xx` / `5xx`)

```json
{
  "error": "Human-readable error message"
}
```

---

## Firebase Storage path

```
lesson_figures/{courseId}/{lessonId}/figure_{index:03d}.png
```

The file is made **public** immediately after upload. Uploading to the same path
overwrites the previous file — callers can safely retry on timeout.

---

## Core logic

The handler must do exactly the following, in order:

### 1. Enrich the description

Before passing the description to PaperBanana, expand it into a detailed diagram brief
using an LLM call. This uses **OpenAI `gpt-4o`**.

**System prompt:**
```
You are a technical diagram director for an educational platform teaching machine learning and AI.

You will be given a short placeholder description of a diagram and the surrounding lesson text
where the diagram will appear.

Write a precise, detailed diagram brief for an AI image generator. Specify:
- What entities, steps, or components should appear
- How they relate or connect (flow, hierarchy, comparison, etc.)
- What labels or annotations are essential for student understanding
- What concept the diagram must make visually obvious

Rules:
- Be specific and visual — describe what should literally appear on the diagram
- Do not describe colours, style, or aesthetics (handled separately)
- 3-6 sentences maximum
- Return ONLY the diagram brief, nothing else
```

**User message:** the raw `description` from the request body.

**Max tokens:** 512

The LLM response is the enriched description passed to PaperBanana.

> If no `OPENAI_API_KEY` is available, skip this step and pass the raw description
> directly. Output quality will be lower.

### 2. Run PaperBanana

```python
import asyncio
from paperbanana import DiagramType, GenerationInput, PaperBananaPipeline
from paperbanana.core.config import Settings

FIGURE_STYLE_CONTEXT = """
Visual style requirements (non-negotiable):
- Teaching-grade clarity. The diagram must be immediately understandable to a student
  encountering this concept for the first time. Prioritise clarity over completeness.
- Subtle, muted colour palette. No bright or saturated colours. Use soft, pastel-adjacent
  tones. The image should feel calm and professional, not eye-catching or vibrant.
- Minimal text. Labels only where essential. No paragraphs, no bullet lists inside the figure.
- Zero visual clutter. Generous whitespace. Every element must earn its place.
- Borders (if used) must be standard width and rendered in a clearly pronounced, dark colour
  so they are unambiguous against the background.
- Spelling is absolutely non-negotiable. Every word, label, and annotation must be spelled
  correctly. A spelling mistake renders the diagram unusable.
"""

settings = Settings(
    vlm_provider="gemini",
    vlm_model="gemini-2.0-flash",
    image_provider="google_imagen",
    image_model="gemini-3-pro-image-preview",
    refinement_iterations=3,
)
pipeline = PaperBananaPipeline(settings=settings)
result = asyncio.run(pipeline.generate(
    GenerationInput(
        source_context=FIGURE_STYLE_CONTEXT + "\n\nDiagram to generate:\n" + enriched_description,
        communicative_intent=enriched_description,
        diagram_type=DiagramType.METHODOLOGY,
    )
))
# result.image_path — absolute path to the generated PNG on local disk
```

### 3. Upload to Firebase Storage

```python
import firebase_admin
from firebase_admin import credentials, storage

app = firebase_admin.initialize_app(
    credentials.Certificate("/secrets/orbit-service-account.json"),
    {"storageBucket": os.environ["ORBIT_STORAGE_BUCKET"]},
)
bucket = storage.bucket(app=app)

storage_path = f"lesson_figures/{course_id}/{lesson_id}/figure_{index:03d}.png"
blob = bucket.blob(storage_path)
blob.upload_from_filename(result.image_path, content_type="image/png")
blob.make_public()
return blob.public_url
```

---

## Environment variables / secrets

| Variable | How to provide | Description |
|---|---|---|
| `GOOGLE_API_KEY` | Secret Manager → env var | Gemini API key (required by PaperBanana) |
| `OPENAI_API_KEY` | Secret Manager → env var | OpenAI key for description enrichment (optional but recommended) |
| `ORBIT_STORAGE_BUCKET` | Plain env var | Firebase Storage bucket, e.g. `your-orbit-app.appspot.com` |
| `ORBIT_SERVICE_ACCOUNT` | Secret Manager → mounted file at `/secrets/orbit-service-account.json` | Orbit Firebase service account JSON |

Mount the service account JSON as a **Secret Manager volume mount** rather than an env var
— the `firebase-admin` SDK reads it from a file path.

---

## Project structure

```
figure-generation-service/
├── main.py                  # Flask app — single POST /generate-figure handler
├── requirements.txt
├── Dockerfile
└── .env.example             # local development only — never committed
```

---

## `requirements.txt`

```
flask
paperbanana
firebase-admin
openai
```

---

## `Dockerfile`

```dockerfile
FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY main.py .

ENV PORT=8080
CMD ["python", "main.py"]
```

---

## Cloud Run configuration

| Setting | Value | Reason |
|---|---|---|
| **Request timeout** | 300s (5 min) | Single image takes ~60–180s; 5 min gives headroom |
| **Memory** | 1 GiB | PaperBanana + Gemini SDK are heavy at import time |
| **CPU** | 1 | Generation is API-bound, not CPU-bound |
| **Minimum instances** | 1 | Eliminates cold start; `paperbanana` import is slow |
| **Maximum instances** | 10 | Caps parallel generation cost |
| **Concurrency** | 1 | PaperBanana uses `asyncio.run()` internally — one request per instance at a time |

---

## Deployment (gcloud)

```bash
# Build and push container
gcloud builds submit --tag gcr.io/YOUR_PROJECT_ID/figure-generation-service

# Deploy
gcloud run deploy figure-generation-service \
  --image gcr.io/YOUR_PROJECT_ID/figure-generation-service \
  --region us-central1 \
  --memory 1Gi \
  --cpu 1 \
  --concurrency 1 \
  --min-instances 1 \
  --max-instances 10 \
  --timeout 300 \
  --set-env-vars ORBIT_STORAGE_BUCKET=your-orbit-app.appspot.com \
  --set-secrets GOOGLE_API_KEY=google-api-key:latest \
  --set-secrets OPENAI_API_KEY=openai-api-key:latest \
  --set-secrets /secrets/orbit-service-account.json=orbit-service-account:latest \
  --no-allow-unauthenticated
```

For internal pipeline use, grant the pipeline's service account the
`roles/run.invoker` role on this service so it can call it without public access.

---

## Notes

- PaperBanana writes the generated PNG to a temp file on disk. Cloud Run provides an
  ephemeral local filesystem under `/tmp` (512 MB default). This is sufficient for a
  single PNG — no extra configuration needed.
- The service has no Firestore dependency. It only touches Firebase Storage.
- `refinement_iterations=3` is the quality setting used across all Vizuara content.
  Lowering it to `1` would halve generation time at the cost of output quality.
