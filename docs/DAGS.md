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
                                     --> download_and_read_agenda (pypdf)
                                           --> extract_session_date
                                               --> extract_agenda_section

[join: trigger_rule=none_failed_min_one_success]
split_srt_by_silence (silencios >= 15s, chunks 10-20 min)
  --> summarize_silence_chunks (GPT-4o-mini)
      --> identify_interesting_chapters (GPT-4o-mini)
          --> merge_interesting_chapters
              --> score_chapter_relevance (GPT-4o-mini)
                  --> save_chapters_to_db (PythonOperator)
```

### Sistema de scoring (0-5)

| Criterio | Puntos | Descripcion |
|---|---|---|
| speaker_relevance_points | 0-2 | Lideres partido/gob: 2 / portavoces o ministros: 1 / otros: 0 |
| topic_relevance_points | 0-2 | Tema candente: 2 / interes medio: 1 / tecnico/administrativo: 0 |
| public_interest_points | 0-1 | Potencial mediatico: 1 / sin interes general: 0 |
| **relevance_score** | **0-5** | **Suma de los tres criterios** |

Umbral para subida: `relevance_score >= 2`. Ese gate vive hoy dentro del join de la vista
`uploadable_turns` (sobre `video_chapters`), que es la que consume el uploader; no es un
umbral suelto de `uploadable_chapters`.

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
<!-- Guarded por tests/congress_videos/test_youtube_upload_dag.py::TestDocsScheduleConsistency -->
**Schedule:** `0 19 * * *` (19:00 UTC, diario)
**Tags:** congress, youtube, chapters
**start_date:** 2025-11-14 | catchup: False | Retries: 1

### Proposito

Publica un turno de orador al dia. Lee la vista `uploadable_turns` mediante
`db.get_uploadable_turns(limit=1)`, genera metadatos con IA, delega la miniatura en
`generic_thumbnail_generator` y sube el video a YouTube a traves del DAG generico.

La unidad de publicacion es el turno de orador, no el capitulo tematico: la seleccion por
capitulo se retiro en #171. La historia del cambio esta en [PIPELINE.md](PIPELINE.md); por
que los nombres del codigo siguen diciendo "chapter", en
[Nomenclatura](#nomenclatura-por-que-los-nombres-dicen-chapter) mas abajo.

### Seleccion

`uploadable_turns` deja como maximo una fila por `output_path` y la ordena internamente
(migracion 044):

```
COALESCE(interest_score, 1) DESC
relevance_score DESC
session_date DESC
materialized_at ASC   -- desempate FIFO (#328)
turn_id ASC           -- backstop de orden total
```

`get_uploadable_turns` ejecuta `SELECT * FROM uploadable_turns LIMIT 1` sin ordenar por
fuera, asi que ese ORDER BY interno es el que decide quien ocupa la unica plaza del dia.

### Parametros

| Parametro | Default | Descripcion |
|---|---|---|
| max_chapters | 1 | Heredado del flujo por capitulos; **el DAG no lo lee**. El tope real es la constante `DAILY_LONG_FORM_UPLOAD_LIMIT = 1` |
| min_relevance_score | 2 | Umbral que usa `check_upload_quota` al contar capitulos pendientes (escala 0-5) |
| isTesting | false | Hardcoded false para subidas publicas |
| dry_run | false | Ejecuta el pipeline completo sin disparar la subida a YouTube |

### Grafo de tareas (14 tareas)

```
ensure_data_directory (PythonOperator)
  --> check_upload_quota (PythonOperator, cuenta subidas de hoy y cola pendiente)
      --> skip_if_quota_reached (ShortCircuitOperator, corta el run si la cola esta vacia)
          --> get_uploadable_item (PythonOperator, uploadable_turns LIMIT 1; siempre item_type="turn")
              --> generate_youtube_metadata (PythonOperator, titulo formato noticia + descripcion)
                  --> prepare_thumbnail_config (PythonOperator, resuelve orador y arma el config)
                      --> generate_thumbnail (PythonOperator, dispara generic_thumbnail_generator y espera)
                          --> extract_chapter_videos (PythonOperator)
                              --> prepare_upload_config (PythonOperator)
                                  --> trigger_youtube_upload (trigger_dag_api + polling 10s)
                                      --> [mark_chapters_uploaded, mark_turns_uploaded]  (en paralelo)
                                          --> backfill_thumbnail_video_id (PythonOperator)
                                              --> check_upload_failures (PythonOperator, fail-loud)
```

Dos tareas de esa cadena son ramas muertas para el flujo actual:

- `extract_chapter_videos` no corta nada cuando `item_type == "turn"`: el MP4 del turno ya
  esta materializado por el DAG `speaker_turn_videos`, asi que la tarea solo reutiliza su
  `output_path`. La rama ffmpeg (`video_splitter.extract_chapters_from_video`) sigue en el
  codigo pero no se alcanza en produccion.
- `mark_chapters_uploaded` marca capitulos; quien marca de verdad es `mark_turns_uploaded`.
  Ambas corren en paralelo, la primera sin filas que tocar.

Ver [Nomenclatura](#nomenclatura-por-que-los-nombres-dicen-chapter).

### Ficheros generados

```
/opt/airflow/data/congress_videos/
  {channel_slug}/{source_video_id}/video_chapters/{chapter_id}/oradores/{output_turn_id}/{filename}
```

El MP4 del turno lo escribe el DAG `speaker_turn_videos`, no este DAG. Aqui se anade la
miniatura (`thumbnail.png`) en ese mismo directorio canonico (#133).

### Composicion de la miniatura

Este DAG ya no compone la miniatura con Pillow. `generate_thumbnail` dispara
`generic_thumbnail_generator` (API Pikzels, direccion de arte, foto del participante
resuelta por slug y titulo por IA) y espera su resultado. La composicion se documenta en
[PIPELINE.md](PIPELINE.md) y en `congress_videos/generic_thumbnail_generator_dag.py`.

### XCom keys

`data_directory_path`, `upload_quota`, `uploadable_item`, `youtube_metadata_results`,
`thumbnail_config`, `thumbnail_dag_run_id`, `thumbnail_result`,
`chapter_extraction_results`, `upload_config`, `upload_results`,
`chapter_upload_updates`, `turn_upload_updates`

### Nomenclatura: por que los nombres dicen "chapter"

Los `task_id` `extract_chapter_videos` / `mark_chapters_uploaded` y las claves XCom
`chapter_extraction_results` / `chapter_upload_updates` conservan el nombre "chapter" a
proposito: son identificadores vivos y no se renombran porque el `task_id` y la clave XCom
son identidad persistida en Airflow (historico de tareas y XCom). Desde #171 transportan
turnos de orador; la rama de capitulo sigue presente en el codigo pero es inalcanzable en
produccion, porque la seleccion solo devuelve `item_type="turn"`.

Lo mismo vale para el propio DAG ID `congress_youtube_chapter_uploader`.

Regla al leer o escribir estos documentos: los identificadores van entre backticks y los
conceptos en palabras normales. Un `chapter_id` es una columna; un capitulo es el tramo
tematico de la sesion. Confundirlos es justo lo que hizo que #325 y #328 se abrieran sobre
un flujo que ya no existia.

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

congress_youtube_chapter_uploader (diario - horario en la seccion 2)
  lee de PostgreSQL: vista uploadable_turns (un turno por run, no subido)
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
