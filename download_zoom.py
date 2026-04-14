import argparse
import subprocess
import sys
from pathlib import Path
from urllib.parse import urlparse

from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError


def build_output_path(output_dir: Path, share_url: str) -> Path:
    parsed = urlparse(share_url)
    # Use the last path segment as a base name, fallback to "recording"
    slug = parsed.path.strip("/").split("/")[-1] or "recording"
    output_dir.mkdir(parents=True, exist_ok=True)
    candidate = output_dir / f"{slug}.mp4"
    counter = 1
    while candidate.exists():
        candidate = output_dir / f"{slug}_{counter}.mp4"
        counter += 1
    return candidate


def cookies_to_header(cookies: list[dict]) -> str:
    return "; ".join(f"{c['name']}={c['value']}" for c in cookies)


def download(share_url: str, password: str, output_dir: Path, headless: bool) -> Path:
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=headless)
        context = browser.new_context()
        page = context.new_page()

        print(f"[1/4] Navigating to share URL...")
        page.goto(share_url, wait_until="networkidle")

        # Handle passcode form if present
        passcode_input = page.query_selector("input#passcode")
        if passcode_input:
            print("[2/4] Passcode form detected — submitting password...")
            passcode_input.fill(password)
            page.click("button#passcode_btn, button[type=submit]")
            try:
                page.wait_for_selector("video", timeout=60_000)
            except PlaywrightTimeoutError:
                browser.close()
                raise RuntimeError("Timed out waiting for video player after passcode submission — check the password or recording availability.")
        else:
            print("[2/4] No passcode form detected — skipping password step.")
            try:
                page.wait_for_selector("video", timeout=60_000)
            except PlaywrightTimeoutError:
                browser.close()
                raise RuntimeError("No passcode form detected and video player did not appear — recording may be expired or unavailable.")

        print("[3/4] Extracting video source URL and cookies...")
        video_src = page.eval_on_selector("video", "el => el.src")
        if not video_src:
            print("ERROR: Could not find video src attribute.")
            browser.close()
            sys.exit(1)
        print(f"      Video URL: {video_src[:80]}...")

        cookies = context.cookies()
        cookie_header = cookies_to_header(cookies)
        referer = page.url

        browser.close()

    output_path = build_output_path(output_dir, share_url)
    print(f"[4/4] Downloading to {output_path} ...")

    curl_cmd = [
        "curl", "-L", "--progress-bar",
        "-H", f"Cookie: {cookie_header}",
        "-H", f"Referer: {referer}",
        "-H", "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
              "AppleWebKit/537.36 (KHTML, like Gecko) "
              "Chrome/124.0.0.0 Safari/537.36",
        "-o", str(output_path),
        video_src,
    ]

    result = subprocess.run(curl_cmd)
    if result.returncode != 0:
        print(f"ERROR: curl exited with code {result.returncode}.")
        sys.exit(result.returncode)

    size_mb = output_path.stat().st_size / 1_048_576
    print(f"Done. Saved {size_mb:.1f} MB → {output_path}")
    return output_path


def main() -> None:
    parser = argparse.ArgumentParser(description="Download a Zoom shared recording.")
    parser.add_argument("url", help="Zoom share link")
    parser.add_argument("password", help="Recording passcode")
    parser.add_argument(
        "--output-dir", default="./downloads", type=Path,
        help="Directory to save the recording (default: ./downloads)",
    )
    parser.add_argument(
        "--no-headless", action="store_true",
        help="Show the browser window (useful for debugging)",
    )
    args = parser.parse_args()

    download(
        share_url=args.url,
        password=args.password,
        output_dir=args.output_dir,
        headless=not args.no_headless,
    )


if __name__ == "__main__":
    main()
