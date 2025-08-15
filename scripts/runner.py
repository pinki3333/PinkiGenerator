# scripts/runner.py
import os
import sys
import json
import base64
import argparse
import subprocess
from pathlib import Path

from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

# Minimal bootstrap: fetch generator script from Google Drive (private) and run it.

SCOPES = ["https://www.googleapis.com/auth/drive.readonly"]

def get_service():
    sa_json = os.environ.get("GCP_SERVICE_ACCOUNT_JSON")
    if not sa_json:
        print("ERROR: GCP_SERVICE_ACCOUNT_JSON not found in env.", file=sys.stderr)
        sys.exit(1)

    try:
        # Accept either plain JSON or base64-encoded JSON:
        if sa_json.strip().startswith("{"):
            info = json.loads(sa_json)
        else:
            info = json.loads(base64.b64decode(sa_json).decode("utf-8"))
    except Exception as e:
        print(f"ERROR: invalid service account JSON: {e}", file=sys.stderr)
        sys.exit(1)

    creds = Credentials.from_service_account_info(info, scopes=SCOPES)
    return build("drive", "v3", credentials=creds, cache_discovery=False)

def download_file(file_id: str, dest: Path):
    svc = get_service()
    request = svc.files().get_media(fileId=file_id)
    from googleapiclient.http import MediaIoBaseDownload
    import io
    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while not done:
        status, done = downloader.next_chunk()
    dest.parent.mkdir(parents=True, exist_ok=True)
    with open(dest, "wb") as f:
        f.write(fh.getvalue())

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--download-only", action="store_true")
    parser.add_argument("--run", action="store_true")
    args = parser.parse_args()

    file_id = os.environ.get("GDRIVE_FILE_ID")
    if not file_id:
        print("ERROR: GDRIVE_FILE_ID not set.", file=sys.stderr)
        sys.exit(1)

    generator_path = Path("generator.py")

    print("Downloading private generator script from Google Drive...")
    download_file(file_id, generator_path)
    print(f"Downloaded to {generator_path.resolve()}")

    if args.download_only and not args.run:
        return

    if args.run:
        # Pass through current environment; generator.py uses env for creds & config.
        print("Running generator.py ...")
        # Use unbuffered mode so logs flush quickly
        env = os.environ.copy()
        cmd = [sys.executable, "-u", str(generator_path)]
        # Pipe output so it shows in Actions logs
        proc = subprocess.Popen(cmd, env=env)
        proc.wait()
        sys.exit(proc.returncode)

if __name__ == "__main__":
    main()
