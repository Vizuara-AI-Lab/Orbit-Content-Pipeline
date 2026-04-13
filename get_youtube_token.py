#!/usr/bin/env python3
"""
Run this script once locally to authorise the YouTube channel and save
youtube-token.json. Copy that file to the VM — no browser needed after that.

Usage:
    python get_youtube_token.py
"""

from pathlib import Path
from google_auth_oauthlib.flow import InstalledAppFlow

_SCOPES       = ["https://www.googleapis.com/auth/youtube.upload"]
_SECRETS_FILE = Path(__file__).parent / "youtube-client-secrets.json"
_TOKEN_FILE   = Path(__file__).parent / "youtube-token.json"

if not _SECRETS_FILE.exists():
    raise FileNotFoundError(f"Missing {_SECRETS_FILE} — download it from Google Cloud Console.")

flow = InstalledAppFlow.from_client_secrets_file(str(_SECRETS_FILE), _SCOPES)
creds = flow.run_local_server(port=0)
_TOKEN_FILE.write_text(creds.to_json())
print(f"Token saved to {_TOKEN_FILE}")
print("Copy this file to the VM before running run_zoom_pipeline.py.")
