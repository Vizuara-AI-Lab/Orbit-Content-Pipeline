PYTHON := .venv/bin/python
PIP    := .venv/bin/pip
WORKERS ?= 4
COURSES_JSON ?= courses-1.json

.PHONY: help install check run content transcripts

help:
	@echo "Usage:"
	@echo "  make install                    Create venv and install all Python dependencies"
	@echo "  make check                      Verify tools and env vars before running"
	@echo "  make run COURSES='course_id_01 course_id_02'"
	@echo "  make content COURSES='course_id_01 course_id_02'"
	@echo "  make transcripts                Run missing transcript pipeline from courses-1.json"
	@echo "  make transcripts WORKERS=8 COURSES_JSON=courses-1.json"
	@echo "  make transcripts COURSES='course_id_01 course_id_02'"

install:
	python3 -m venv .venv
	$(PIP) install --upgrade pip
	$(PIP) install firebase-admin openai yt-dlp python-dotenv paperbanana
	@echo ""
	@echo "Python dependencies installed."
	@echo ""
	@echo "  Reminder: ffmpeg must be installed separately."
	@echo "  On Debian/Ubuntu:  sudo apt install -y ffmpeg"

check:
	@echo "--- Checking tools ---"
	@$(PYTHON) --version
	@ffmpeg -version 2>&1 | head -1
	@.venv/bin/yt-dlp --version
	@echo ""
	@echo "--- Checking env vars ---"
	@$(PYTHON) check_env.py

run:
	$(PYTHON) run_pipeline.py $(COURSES)

content:
	$(PYTHON) run_content_pipeline.py $(COURSES)

transcripts:
ifdef COURSES
	$(PYTHON) run_transcript_pipeline.py --workers $(WORKERS) $(COURSES)
else
	$(PYTHON) run_transcript_pipeline.py --workers $(WORKERS) --from-json $(COURSES_JSON)
endif
