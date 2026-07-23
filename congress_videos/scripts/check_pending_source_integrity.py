"""
Check source-file integrity for videos pending chapter upload.

Queries the ``uploadable_chapters`` view for chapters not yet uploaded,
resolves each unique source video on disk (same lookup logic as
``extract_chapters_from_video``), and runs a full ffmpeg decode-to-null
pass per source to detect corrupted files before extraction runs.

Usage (inside the airflow container, same env as the DAGs):
    conda run -n airflow python congress_videos/scripts/check_pending_source_integrity.py
    conda run -n airflow python congress_videos/scripts/check_pending_source_integrity.py --min-relevance-score 4
"""

import argparse
import os
import subprocess
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from congress_videos.modules.database import CongressionalVideoDB  # noqa: E402
from utils.airflow_helpers import ensure_project_data_directory  # noqa: E402


def find_source_video(data_directory: str, video_id: str) -> str | None:
    """Locate the source video file, mirroring extract_chapters_from_video's lookup."""
    downloads_folder = os.path.join(data_directory, 'downloads')
    if not os.path.exists(downloads_folder):
        return None

    video_extensions = ('.mp4', '.mkv', '.webm')
    for date_folder in os.listdir(downloads_folder):
        video_folder = os.path.join(downloads_folder, date_folder, str(video_id))
        if not os.path.exists(video_folder):
            continue
        for file in os.listdir(video_folder):
            if file.endswith(video_extensions) and 'chapter_video' not in file:
                return os.path.join(video_folder, file)
    return None


def check_integrity(source_path: str) -> tuple[bool, int]:
    """Full decode-to-null pass. Returns (ok, error_line_count)."""
    result = subprocess.run(
        [
            'ffmpeg', '-v', 'error', '-err_detect', 'ignore_err',
            '-i', source_path, '-f', 'null', '-',
        ],
        capture_output=True,
        text=True,
    )
    error_lines = [line for line in result.stderr.splitlines() if line.strip()]
    return result.returncode == 0, len(error_lines)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--min-relevance-score', type=int, default=4)
    parser.add_argument('--limit', type=int, default=None)
    args = parser.parse_args()

    db = CongressionalVideoDB()
    chapters = db.get_uploadable_chapters(
        limit=args.limit, min_relevance_score=args.min_relevance_score
    )

    if not chapters:
        print("No pending uploadable chapters found.")
        return

    video_ids = sorted({c['video_id'] for c in chapters})
    print(f"{len(chapters)} pending chapters across {len(video_ids)} source video(s).\n")

    data_directory = ensure_project_data_directory('congress_videos')

    results = []
    for video_id in video_ids:
        source_path = find_source_video(data_directory, video_id)
        if not source_path:
            print(f"[MISSING] {video_id}: no source file found on disk")
            results.append((video_id, 'MISSING', None))
            continue

        print(f"Checking {video_id} ({os.path.basename(source_path)}) ...", flush=True)
        ok, error_count = check_integrity(source_path)
        status = 'OK' if ok else 'CORRUPT'
        print(f"  -> {status} ({error_count} decode error lines)")
        results.append((video_id, status, error_count))

    print("\n--- Summary ---")
    for video_id, status, error_count in results:
        suffix = f" ({error_count} errors)" if error_count is not None else ""
        print(f"{video_id}: {status}{suffix}")

    corrupt = [r for r in results if r[1] in ('CORRUPT', 'MISSING')]
    if corrupt:
        print(f"\n{len(corrupt)} video(s) need attention: {[r[0] for r in corrupt]}")
        sys.exit(1)


if __name__ == '__main__':
    main()
