python := if os_family() == "windows" { ".venv/Scripts/python" } else { ".venv/bin/python" }
pip    := if os_family() == "windows" { ".venv/Scripts/pip" } else { ".venv/bin/pip" }

# List available recipes
default:
    @just --list

# Create venv and install all Python dependencies
install:
    python3 -m venv .venv
    {{ pip }} install --upgrade pip
    {{ pip }} install firebase-admin openai yt-dlp python-dotenv paperbanana
    @echo ""
    @echo "✓ Python dependencies installed."
    @echo ""
    @echo "  Reminder: ffmpeg must be installed separately as a system package."
    @echo "  On Debian/Ubuntu:  sudo apt install -y ffmpeg"

# Run the migration pipeline for one or more course IDs (space-separated)
# Usage: just run course_id_01 course_id_02
run *course_ids:
    {{ python }} run_pipeline.py {{ course_ids }}

# Run the content pipeline (transcription → summary → quiz) on prod lessons
# Usage: just content course_id_01 course_id_02
content *course_ids:
    {{ python }} run_content_pipeline.py {{ course_ids }}

# Verify that all required tools and env vars are present before running
check:
    @echo "--- Checking tools ---"
    @{{ python }} --version
    @ffmpeg -version 2>&1 | head -1
    @.venv/bin/yt-dlp --version 2>/dev/null || .venv/Scripts/yt-dlp --version 2>/dev/null || echo "yt-dlp: OK (check venv activation)"
    @echo ""
    @echo "--- Checking env vars ---"
    @{{ python }} check_env.py
