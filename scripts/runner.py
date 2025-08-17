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
from googleapiclient.http import MediaIoBaseDownload

SCOPES = ["https://www.googleapis.com/auth/drive.readonly"]

def get_service():
    sa_json = os.environ.get("GCP_SERVICE_ACCOUNT_JSON")
    if not sa_json:
        print("ERROR: GCP_SERVICE_ACCOUNT_JSON not found in env.", file=sys.stderr)
        sys.exit(1)

    try:
        if sa_json.strip().startswith("{"):
            info = json.loads(sa_json)
        else:
            info = json.loads(base64.b64decode(sa_json).decode("utf-8"))
    except Exception as e:
        print(f"ERROR: invalid service account JSON: {e}", file=sys.stderr)
        sys.exit(1)

    creds = Credentials.from_service_account_info(info, scopes=SCOPES)
    return build("drive", "v3", credentials=creds)

def download_file(file_id: str, dest: Path):
    svc = get_service()
    request = svc.files().get_media(fileId=file_id)

    tmp = dest.with_suffix(".tmp")
    with open(tmp, "wb") as f:
        downloader = MediaIoBaseDownload(f, request)
        done = False
        while not done:
            status, done = downloader.next_chunk()
            if status:
                print(f"Download progress: {int(status.progress() * 100)}%")
    tmp.replace(dest)

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
        print("Running generator.py ...")
        env = os.environ.copy()
        cmd = [sys.executable, "-u", str(generator_path)]
        proc = subprocess.Popen(cmd, env=env)
        returncode = proc.wait()
        sys.exit(returncode)

if __name__ == "__main__":
    main()
