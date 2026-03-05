#!/usr/bin/env python3
"""Pre-flight env var check — called by `just check`."""
import os
import sys
from dotenv import load_dotenv

load_dotenv()

required = ["OPENAI_API_KEY", "GOOGLE_API_KEY", "ORBIT_STORAGE_BUCKET"]
missing = [v for v in required if not os.getenv(v)]

if missing:
    print("Missing env vars:", missing)
    sys.exit(1)

print("All required env vars are set.")
