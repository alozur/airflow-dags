# DAGs - Documentacion detallada

---

## 1. congress_youtube_channel_monitor

**Fichero:** congress_videos/youtube_channel_monitor_dag.py
**DAG ID:** congress_youtube_channel_monitor
**Schedule:** `0 22 * * *` (22:00 hora Madrid, diario)
**Tags:** congress, youtube, monitor
**start_date:** 2025-10-09 | catchup: False | Retries: 2 (delay 5 min)

### Proposito

Monitoriza el canal oficial @CanalParlamento-Congreso_Es, detecta la sesion plenaria
del dia, la transcribe con Whisper o descarga subtitulos de YouTube, divide el contenido
en capitulos semanticos con GPT-4o-mini, los puntua (0-5) y guarda en PostgreSQL.

### Parametros

| Parametro | Default | Descripcion |
|---|---|---|
| target_date | ayer YYYY-MM-DD | Fecha de la sesion a procesar |
| max_videos | 20 | Max videos a consultar en el canal |
| chunk_duration_minutes | 30 | Duracion chunk audio para Whisper |
| isTesting | false | Si true usa test_video_url |
| test_video_url | URL fija | Video YouTube en modo test |

### Grafo de tareas

```
check_test_mode (BranchPythonOperator)
  test --> create_test_video_data
  prod --> fetch_youtube_channel_videos
             --> filter_plenary_sessions
                 --> check_if_plenary_found (BranchPythonOperator)
                       no  --> no_plenary_sessions [FIN]
                       si  --> [Paralelo]
                               get_video_details
                                 --> try_download_subtitles_from_youtube (yt-dlp)
                                     --> check_subtitles_available (BranchPythonOperator)
                                           si  --> split_srt_by_silence
                                           no  --> extract_audio_from_youtube
                                                    --> transcribe_audio_with_whisper
                                                        --> merge_srt_files
                                                            --> split_srt_by_silence
                                 --> download_video_from_youtube (MP4 completo)
                               get_video_descriptions
                                 --> parse_description_links
                                     --> scrape_press_release (BeautifulSoup)
                                     --> download_and_read_agenda (PyPDF2)
                                           --> extract_session_date
                                               --> extract_agenda_section

[join: trigger_rule=none_failed_min_one_success]
split_srt_by_silence (silencios >= 15s, chunks 10-20 min)
  --> summarize_silence_chunks (GPT-4o-mini)
      --> identify_interesting_chapters (GPT-4o-mini)
          --> merge_interesting_chapters
              --> score_chapter_relevance (GPT-4o-mini)
                  --> save_chapters_to_db (PostgreSQLOperator)
```

### Sistema de scoring (0-5)

| Criterio | Puntos | Descripcion |
|---|---|---|
| speaker_relevance_points | 0-2 | Lideres partido/gob: 2 / portavoces o ministros: 1 / otros: 0 |
| topic_relevance_points | 0-2 | Tema candente: 2 / interes medio: 1 / tecnico/administrativo: 0 |
| public_interest_points | 0-1 | Potencial mediatico: 1 / sin interes general: 0 |
| **relevance_score** | **0-5** | **Suma de los tres criterios** |

Umbral para subida: `relevance_score >= 2` (vista `uploadable_chapters`).

### XCom keys producidas

`channel_videos`, `plenary_videos`, `video_details`, `youtube_subtitles`,
`extracted_audio`, `transcriptions`, `merged_srt_files`, `silence_chunks`,
`downloaded_videos`, `video_descriptions`, `parsed_links`, `press_releases`,
`agendas`, `session_date`, `agenda_section`, `chunk_summaries`,
`identified_chapters`, `interesting_chapters`, `scored_chapters`, `db_save_results`

---

## 2. congress_youtube_chapter_uploader

**Fichero:** congress_videos/youtube_upload_dag.py
**DAG ID:** congress_youtube_chapter_uploader
**Schedule:** `0 12 * * *` (12:00 hora Madrid, diario)
**Tags:** congress, youtube, chapters
**start_date:** 2025-11-14 | catchup: False | Retries: 1

### Proposito

Toma los capitulos con mayor puntuacion de la vista `uploadable_chapters`, genera
metadatos con IA, crea miniaturas con Pillow, extrae fragmentos de video con ffmpeg y
los sube a YouTube a traves del DAG generico.

### Parametros

| Parametro | Default | Descripcion |
|---|---|---|
| max_chapters | 5 | Max capitulos a subir por ejecucion |
| min_relevance_score | 2 | Puntuacion minima (escala 0-5) |
| isTesting | false | Hardcoded false para subidas publicas |

### Grafo de tareas (secuencial)

```
ensure_data_directory (PythonOperator)
  --> get_uploadable_chapters (PostgreSQLOperator)
      --> generate_youtube_metadata (PythonOperator, GPT-3.5-turbo)
          --> generate_thumbnail_text (PythonOperator, GPT-3.5-turbo, 3-6 palabras max 40 chars)
              --> generate_thumbnails (PythonOperator, Pillow 1280x720)
                  --> extract_chapter_videos (PythonOperator, ffmpeg input-seek + re-encode libx264/aac, frame-accurate; escribe chapter_video.mp4, original intacto)
                      --> prepare_upload_config (PythonOperator)
                          --> trigger_youtube_upload (trigger_dag_api + polling 10s)
                              --> mark_chapters_uploaded (PostgreSQLOperator)
```

### Ficheros generados

```
/opt/airflow/data/congress_videos/
  {video_id}/{chapter_id}/chapter_video.mp4   (ffmpeg -c copy, sin re-encode)
  {video_id}/{chapter_id}/thumbnail.png       (Pillow 1280x720)
```

### Composicion de la miniatura

- Fondo: imagen hemiciclo Congreso oscurecida al 70%
- Borde dorado `#FFD700`, 8px
- Texto central en MAYUSCULAS, 3-6 palabras, max 40 chars (hasta 5 intentos IA)
- Numero de sesion en dorado en zona inferior
- Logo circular en esquina superior izquierda (6% del ancho)
- Fuentes: `LiberationSans-Bold` / `LiberationSans-Regular`

### XCom keys

`data_directory_path`, `uploadable_chapters`, `youtube_metadata_results`,
`thumbnail_text_results`, `thumbnail_results`, `chapter_extraction_results`,
`upload_config`, `upload_results`, `chapter_upload_updates`

---

## 3. generic_youtube_uploader

**Fichero:** utils/youtube_uploader_dag.py
**DAG ID:** generic_youtube_uploader
**Schedule:** None (solo trigger)
**Tags:** youtube, upload, generic, utils
**max_active_runs:** 3 | Retries: 2

### Proposito

DAG reutilizable para subir videos a YouTube. No tiene schedule propio;
se lanza mediante `trigger_dag_api` desde otro DAG. Soporta hasta 3 ejecuciones paralelas.

### Configuracion dag_run.conf

```json
{
  "token_file": "/opt/airflow/data/congress_videos/congress_youtube_token.pickle",
  "videos": [
    {
      "video_file": "/path/to/video.mp4",
      "title": "Titulo del video",
      "description": "Descripcion larga...",
      "category_id": "25",
      "privacy_status": "public",
      "tags": ["congreso", "parlamento"],
      "thumbnail_file": "/path/to/thumbnail.png",
      "chapter_id": 42,
      "video_id": "abc123xyz"
    }
  ]
}
```

### Grafo de tareas

```
validate_config --> upload_videos
```

- `validate_config`: verifica `token_file`, lista no vacia y campos requeridos por video.
- `upload_videos`: sube todos los videos con OAuth token pickle (auto-refresco del token).

### XCom keys de salida

`upload_results`: lista de resultados por video con `youtube_video_id` o mensaje de error.

---

## 4. git_sync_dag

**Fichero:** utils/git_sync_dag.py
**DAG ID:** git_sync_dag
**Schedule:** None (solo manual)
**Tags:** utility, git, sync

### Proposito

Sincroniza manualmente el repositorio de DAGs desde GitHub sin reiniciar Docker.
Alternativa ligera al contenedor continuo de git-sync.

### Grafo de tareas

```
configure_git (BashOperator)
  --> git_pull (BashOperator)
      --> show_status (BashOperator)
```
- configure_git: configura credential.helper e identidad git en /opt/airflow/dags/repo
- git_pull: git fetch origin + git reset --hard origin/{GIT_SYNC_BRANCH}
- show_status: imprime la rama activa y el hash del ultimo commit

### Variables de entorno requeridas

GITHUB_USER, GITHUB_TOKEN, GITHUB_REPO, GIT_SYNC_BRANCH (default: dev)

---

## Relacion entre DAGs

congress_youtube_channel_monitor (22:00)
  escribe en PostgreSQL: video_chapters con relevance_score 0-5

congress_youtube_chapter_uploader (12:00 dia siguiente)
  lee de PostgreSQL: vista uploadable_chapters (score >= 2, no subidos)
  trigger_dag_api --> generic_youtube_uploader (polling cada 10s)
  escribe en PostgreSQL: is_uploaded_to_youtube=TRUE, youtube_video_id

generic_youtube_uploader (on-demand)
  sube video a YouTube via YouTube Data API v3 + OAuth
  devuelve resultados via XCom.get_many al DAG padre

git_sync_dag (manual)
  independiente, no interactua con ningun otro DAG

---

## Modo test vs produccion

| Aspecto | isTesting=true | isTesting=false (produccion) |
|---|---|---|
| Origen video | URL fija (test_video_url) | Canal @CanalParlamento-Congreso_Es |
| Primera rama del Branch | create_test_video_data | fetch_youtube_channel_videos |
| Activacion | Manual desde la UI | Automatico 22:00 |

Activar modo test desde la UI de Airflow:
Trigger DAG > Trigger with config > introducir:
{"isTesting": true, "test_video_url": "https://www.youtube.com/watch?v=VIDEO_ID"}
