# Arquitectura

## Vision general

Pipeline de automatizacion de contenido politico. Orquestado por Apache Airflow,
consume el canal oficial de YouTube del Congreso de los Diputados, procesa las
sesiones plenarias con IA y republica los fragmentos mas relevantes como videos
independientes de YouTube.

---

## Diagrama de arquitectura (ASCII)

  APACHE AIRFLOW (Docker, LocalExecutor, Europe/Madrid)
  |
  +-- congress_youtube_channel_monitor  (22:00 diario)
  |     Branch: check_test_mode
  |       |-- test: create_test_video_data
  |       +-- prod: fetch_youtube_channel_videos (YouTube Data API, eventType=completed)
  |     --> filter_plenary_sessions
  |     Branch: check_if_plenary_found
  |       |-- no: no_plenary_sessions (fin limpio)
  |       +-- si:
  |             [Paralelo]
  |               get_video_details
  |                 --> try_download_subtitles_from_youtube (yt-dlp)
  |                     Branch: check_subtitles_available
  |                       |-- si: split_srt_by_silence
  |                       +-- no: extract_audio_from_youtube
  |                                --> transcribe_audio_with_whisper (Whisper)
  |                                    --> merge_srt_files
  |                                        --> split_srt_by_silence
  |                 --> download_video_from_youtube (MP4 completo)
  |               get_video_descriptions
  |                 --> parse_description_links
  |                     [Paralelo]
  |                       scrape_press_release (BeautifulSoup)
  |                       download_and_read_agenda (pypdf)
  |                         --> extract_session_date
  |                             --> extract_agenda_section
  |     --> split_srt_by_silence (silencios >= 15s, chunks 10-20 min)
  |     --> summarize_silence_chunks (GPT-4o-mini)
  |     --> identify_interesting_chapters (GPT-4o-mini)
  |     --> merge_interesting_chapters
  |     --> score_chapter_relevance (GPT-4o-mini, score 0-5)
  |     --> save_chapters_to_db (PostgreSQL)
  |
  +-- congress_youtube_chapter_uploader  (19:00 UTC diario)
  |     ensure_data_directory
  |     check_upload_quota --> skip_if_quota_reached (corta si el run llega tarde,
  |         si ya se subio el cupo del dia o si la cola esta vacia)
  |     get_uploadable_item (vista uploadable_turns, LIMIT 1; siempre item_type="turn")
  |     generate_youtube_metadata (titulo formato noticia + descripcion)
  |     prepare_thumbnail_config --> generate_thumbnail (dispara generic_thumbnail_generator)
  |     extract_chapter_videos (turno: reutiliza el MP4 ya materializado; rama ffmpeg inalcanzable)
  |     prepare_upload_config
  |     trigger_youtube_upload --> generic_youtube_uploader (polling)
  |     [mark_chapters_uploaded, mark_turns_uploaded] (PostgreSQL UPDATE, en paralelo)
  |     backfill_thumbnail_video_id --> check_upload_failures
  |
  +-- generic_youtube_uploader  (sin schedule, solo trigger)
  |     validate_config --> upload_videos (YouTube Data API v3, OAuth)
  |
  +-- git_sync_dag  (sin schedule, manual)
        configure_git --> git_pull --> show_status

  Servicios externos:
    PostgreSQL   (red: postgres_infra_network)
    YouTube API  (lectura canal + subida via OAuth)
    OpenAI API   (GPT-4o-mini, GPT-3.5-turbo)
    Whisper ASR  (local library o Docker en whisper_network:9000)

  NAS montado en /opt/airflow/data/congress_videos/:
    downloads/{date}/{video_id}/audio_chunks/  (audio WebM)
    downloads/{date}/{video_id}/srt_files/     (subtitulos SRT)
    {channel_slug}/{source_video_id}/video_chapters/{chapter_id}/subtitles.srt
      (sidecar SRT del capitulo, #340: timestamps absolutos del video fuente)
    {channel_slug}/{source_video_id}/video_chapters/{chapter_id}/oradores/{output_turn_id}/{filename}
      (layout canonico #133: video.mp4, thumbnail.png, subtitles.srt)
    {channel_slug}/{source_video_id}/video_chapters/{chapter_id}/shorts/{clip_id}.mp4 + {clip_id}.srt
      (sidecar SRT del short, #431: aproximacion a la ventana de pre-recorte del
      capitulo re-temporizada al origen del clip; Reap no expone tiempos por clip)
    assets/  (background, logo, fonts)
    congress_youtube_token.pickle


---

## Flujo XCom entre tareas

Patron uniforme via utils/airflow_helpers.py::xcom_task:

    t = PythonOperator(
        task_id='fetch_videos',
        python_callable=lambda ti, **ctx: xcom_task(
            ti,
            lambda: fetch_youtube_channel_videos(channel_id=CHANNEL_ID),
            'channel_videos'          # XCom key de salida
        ),
    )

    t2 = PythonOperator(
        task_id='filter_sessions',
        python_callable=lambda ti, **ctx: xcom_task(
            ti,
            lambda: filter_plenary_session_videos(
                ti.xcom_pull(key='channel_videos'),   # XCom key de entrada
                target_date=ctx["params"]["target_date"]
            ),
            'plenary_videos'
        ),
    )

XCom keys principales en congress_youtube_channel_monitor:
  channel_videos, plenary_videos, video_details, youtube_subtitles,
  extracted_audio, transcriptions, merged_srt_files, silence_chunks,
  downloaded_videos, video_descriptions, parsed_links, press_releases,
  agendas, session_date, agenda_section, chunk_summaries,
  identified_chapters, interesting_chapters, scored_chapters, db_save_results

XCom keys en congress_youtube_chapter_uploader:
  data_directory_path, upload_quota, uploadable_item, youtube_metadata_results,
  thumbnail_config, thumbnail_dag_run_id, thumbnail_result,
  chapter_extraction_results, upload_config, upload_results,
  chapter_upload_updates, turn_upload_updates

  chapter_extraction_results y chapter_upload_updates son los nombres literales de
  las claves y transportan turnos de orador desde #171: la clave XCom es identidad
  persistida en Airflow y no se renombra. Ver "Nomenclatura" en DAGS.md.


---

## Capas de arquitectura

  DAG layer
    congress_videos/youtube_channel_monitor_dag.py
    congress_videos/youtube_upload_dag.py
    utils/git_sync_dag.py
    utils/youtube_uploader_dag.py

  DB task layer
    Direct CongressionalVideoDB callables inside each DAG (PythonOperator).
    No custom operator indirection (issue #227): each task pulls its own
    XCom input, calls the DB method, pushes its own output_xcom_key.

  Business logic layer
    congress_videos/modules/youtube/download.py
    congress_videos/modules/youtube/youtube_channel.py
    congress_videos/modules/youtube/youtube_ai.py
    congress_videos/modules/youtube/youtube_upload.py
    congress_videos/modules/database.py  (CongressionalVideoDB)
    congress_videos/modules/video_splitter.py
    congress_videos/modules/web_scraping.py
    congress_videos/modules/speaker_helpers.py

  Shared utils layer
    utils/airflow_helpers.py    xcom_task, ensure_project_data_directory
    utils/ai_helpers.py         generate_chat_completion, generate_json_completion
    utils/ai_chapter_analyzer.py  detect_silence_gaps, chunk_by_silence
    utils/whisper_helpers.py    transcribe_audio_file, merge_srt_files
    utils/youtube_downloader.py download_youtube_video_for_upload, download_audio_in_chunks
    utils/youtube_helpers.py    upload_multiple_videos, validate_upload_config
    utils/postgres_helpers.py   PostgresConnection
    utils/env_loader.py         load_env_if_local

  Config layer
    congress_videos/config/ai_prompts.py    todos los prompts IA centralizados
    congress_videos/config/constants.py     YOUTUBE_CHANNEL_ID, TARGET_VIDEO_TITLE
    congress_videos/config/paths.py         BASE_DATA_DIR, rutas helper functions


---

## Schema de base de datos

  Schema: development | production  (segun env var POSTGRES_SCHEMA)

  congressional_sessions
    session_number PK, session_date, target_date, session_url, total_topics

  video_topics
    entry_id PK, session_number FK, topic_title, video_url, speaker_name, role,
    is_main_topic, upload_eligible, ai_interest_score, youtube_title,
    youtube_description, thumbnail_path, is_uploaded_to_youtube, youtube_video_id

  upload_queue
    video_topic_entry_id PK/FK, queue_priority, upload_status, attempted_uploads

  youtube_source_videos
    video_id PK, video_title, video_url, session_number, session_date,
    duration_seconds, total_chapters, is_processed

  video_chapters
    chapter_id SERIAL PK, video_id FK,
    title, description, start_time, end_time, duration_minutes,
    speakers[] (array), topics[] (array),
    relevance_score 0-5, speaker_relevance_points 0-2,
    topic_relevance_points 0-2, public_interest_points 0-1,
    scoring_reasoning, key_speakers[], is_current_topic,
    is_uploaded_to_youtube, youtube_video_id,
    resolved_participant_slug, mentioned_participant_slugs[]

    Tres conceptos distintos, resueltos por caminos independientes (issue #432):
    - resolved_participant_slug: quien HABLA (orador del capítulo), issue #263.
    - mentioned_participant_slugs[]: a quien se MENCIONA en la transcripción,
      congress_videos/modules/mentioned_people_resolution.py, roster-gated,
      migración 045. NULL = sin analizar; array vacío = analizado, sin menciones.
    - topics[]: de que TRATA el capítulo. Escrito originalmente por
      utils/ai_chapter_analyzer.py en la identificación de capítulos; desde el
      issue #432 la fuente de verdad se refresca en tiempo de subida por
      congress_videos/modules/topic_extraction.py (independiente de
      mentioned_participant_slugs, propia llamada LLM y cache). Una extracción
      exitosa sin temas NO sobrescribe un valor previo no vacío.

  Vistas:
    uploadable_chapters  relevance_score >= 2 AND is_uploaded = FALSE
                         lector: congress_videos/speaker_turns_dag.py::select_chapters()
                         y congress_videos/scripts/check_pending_source_integrity.py
    uploadable_turns     turnos con prepared_at set y sin is_uploaded_to_youtube
                         lector: congress_videos/youtube_upload_dag.py via
                         congress_videos/modules/database.py::get_uploadable_turns
    chapter_statistics   agregados por video fuente
    uploadable_videos    pipeline legado de video_topics


---

## Patrones de diseno

BranchPythonOperator (3 bifurcaciones en monitor DAG):
  check_test_mode, check_if_plenary_found, check_subtitles_available

trigger_rule='none_failed_min_one_success':
  Tareas que deben ejecutarse tras cualquiera de dos ramas posibles del Branch

DAG-to-DAG triggering con polling:
  trigger_dag_api + loop time.sleep(10) + XCom.get_many para recuperar resultados

Prompts centralizados:
  congress_videos/config/ai_prompts.py, sin mezclar con logica de negocio

Schema dinamico por entorno:
  PostgresConnection.get_qualified_table(name) -> {POSTGRES_SCHEMA}.{name}

Fallback de transcripcion (3 niveles):
  1. Subtitulos SRT de YouTube (yt-dlp, mas rapido)
  2. Audio chunks + Whisper local (openai-whisper library, genera SRT)
  3. Whisper Docker API (texto plano, sin timestamps)
