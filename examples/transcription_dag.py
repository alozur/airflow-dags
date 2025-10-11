"""
YouTube Audio Transcription DAG Example.

This DAG demonstrates the complete workflow:
1. Download audio from YouTube video
2. Transcribe using Whisper API
3. Save transcription to file
4. Clean up temporary files

Schedule: Manual trigger only
Owner: data_team
"""

from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator

from utils.airflow_helpers import xcom_task
from utils.whisper_helpers import transcribe_and_save
from utils.youtube_downloader import download_audio_only

# Constants
DAG_ID = "youtube_transcription_example"
DEFAULT_ARGS = {
    "owner": "data_team",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=1),
}


@xcom_task
def download_youtube_audio(**context):
    """
    Download audio from YouTube video.

    Returns:
        Dictionary with audio file path and metadata
    """
    # Get YouTube URL from DAG params or use default
    youtube_url = context.get("params", {}).get(
        "youtube_url", "https://www.youtube.com/watch?v=dQw4w9WgXcQ"
    )

    # Define output directory (inside Airflow container)
    output_dir = "/opt/airflow/data/congress_videos/audio"

    result = download_audio_only(
        youtube_url=youtube_url,
        output_dir=output_dir,
        audio_format="webm",  # WebM is lightweight and works well with Whisper
    )

    if not result["success"]:
        raise Exception(f"Audio download failed: {result['error']}")

    return {
        "audio_file": result["file_path"],
        "file_size_mb": result["file_size_mb"],
        "duration": result["duration"],
    }


@xcom_task
def transcribe_audio_task(**context):
    """
    Transcribe audio using Whisper API.

    Returns:
        Dictionary with transcription result
    """
    # Get audio file path from previous task
    ti = context["task_instance"]
    download_result = ti.xcom_pull(task_ids="download_audio")

    if not download_result:
        raise Exception("No audio file from download task")

    audio_file = download_result["audio_file"]

    # Get language from params (default: auto-detect)
    language = context.get("params", {}).get("language", None)

    # Define output file path
    audio_path = Path(audio_file)
    output_file = (
        f"/opt/airflow/data/congress_videos/transcripts/{audio_path.stem}.txt"
    )

    result = transcribe_and_save(
        audio_file_path=audio_file,
        output_file_path=output_file,
        whisper_api_url="http://whisper-api:9000",
        language=language,
        include_timestamps=True,
    )

    if not result["success"]:
        raise Exception(f"Transcription failed: {result['error']}")

    return {
        "transcription_file": result["output_file"],
        "transcription_text": result["text"],
        "language": result["language"],
        "duration": result["duration"],
    }


def cleanup_audio_file(**context):
    """
    Clean up temporary audio file to save space.
    """
    ti = context["task_instance"]
    download_result = ti.xcom_pull(task_ids="download_audio")

    if download_result:
        audio_file = download_result["audio_file"]
        audio_path = Path(audio_file)

        if audio_path.exists():
            audio_path.unlink()
            print(f"✅ Deleted temporary audio file: {audio_file}")
        else:
            print(f"⚠️ Audio file not found: {audio_file}")


def log_summary(**context):
    """
    Log summary of transcription results.
    """
    ti = context["task_instance"]

    download_result = ti.xcom_pull(task_ids="download_audio")
    transcribe_result = ti.xcom_pull(task_ids="transcribe_audio")

    print("\n" + "=" * 60)
    print("TRANSCRIPTION SUMMARY")
    print("=" * 60)

    if download_result:
        print(f"Audio File Size: {download_result['file_size_mb']} MB")
        print(f"Audio Duration: {download_result['duration']} seconds")

    if transcribe_result:
        print(f"Language: {transcribe_result['language']}")
        print(f"Transcription File: {transcribe_result['transcription_file']}")
        print(f"Text Length: {len(transcribe_result['transcription_text'])} chars")
        print(f"\nFirst 200 characters:")
        print(transcribe_result["transcription_text"][:200] + "...")

    print("=" * 60 + "\n")


# DAG definition
with DAG(
    dag_id=DAG_ID,
    default_args=DEFAULT_ARGS,
    description="Download YouTube audio and transcribe with Whisper",
    schedule=None,  # Manual trigger only
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["youtube", "transcription", "whisper", "example"],
    params={
        "youtube_url": "https://www.youtube.com/watch?v=dQw4w9WgXcQ",
        "language": None,  # Auto-detect language (or use: 'es', 'en', 'ca', etc.)
    },
) as dag:

    # Task 1: Download audio from YouTube
    download_task = PythonOperator(
        task_id="download_audio",
        python_callable=download_youtube_audio,
    )

    # Task 2: Transcribe audio with Whisper
    transcribe_task = PythonOperator(
        task_id="transcribe_audio",
        python_callable=transcribe_audio_task,
    )

    # Task 3: Log summary
    summary_task = PythonOperator(
        task_id="log_summary",
        python_callable=log_summary,
    )

    # Task 4: Clean up temporary files
    cleanup_task = PythonOperator(
        task_id="cleanup_audio",
        python_callable=cleanup_audio_file,
        trigger_rule="all_done",  # Run even if previous tasks fail
    )

    # Define task dependencies
    download_task >> transcribe_task >> summary_task >> cleanup_task
