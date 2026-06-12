# Auditoría: simplificación de `youtube_channel_monitor_dag`

**Objetivo**: eliminar la rama de scraping de congreso.es y dejar el DAG monitorizando únicamente el canal de YouTube del parlamento.

---

## Estado actual

El DAG tiene **25 tasks** con dos pipelines entrelazados:

```
YouTube API → filtrar sesiones → descargar/transcribir → IA scoring → BD   ← CONSERVAR
                                        ↓
                        get_video_descriptions → parse_description_links
                                                         ↓
                             scrape_press_release    download_and_read_agenda
                                     ↓                        ↓
                             (sin consumidor)       extract_session_date → extract_agenda_section
                                                            ↓
                                                    save_chapters_to_db (session_date XCom)
```

### Rama congreso — código muerto

| Task | Problema |
|------|----------|
| `scrape_press_release` | Sin downstream — output nunca consumido |
| `extract_agenda_section` | Hoja colgante — nunca conectada |
| `get_video_descriptions` | Solo existía para alimentar `parse_description_links` |

---

## Cambios necesarios

### 1. Eliminar tasks del DAG (`youtube_channel_monitor_dag.py`)

| # | Task ID | Motivo |
|---|---------|--------|
| 1 | `get_video_descriptions` | Dead end sin la rama congreso |
| 2 | `parse_description_links` | Puerta de entrada a la rama congreso |
| 3 | `scrape_press_release` | Scraper BeautifulSoup congreso.es, sin downstream |
| 4 | `download_and_read_agenda` | Descargador PDF via PyPDF2 |
| 5 | `extract_session_date` | Parser de fechas en castellano (183 líneas) |
| 6 | `extract_agenda_section` | Hoja colgante, nunca consumida |

### 2. Actualizar edges de dependencias

```python
# Antes
t2a >> [t3a, t3b]
t0_test >> [t3a, t3b]
t3b >> t4
t4 >> [t5a, t5b]
t5b >> t5c >> t5d
[t8, t5c] >> t9_db

# Después
t2a >> t3a
t0_test >> t3a
t8 >> t9_db
```

### 3. Eliminar funciones de `modules/youtube/youtube_channel.py`

- `parse_description_links()`
- `scrape_press_release()`
- `download_and_read_agenda()`
- `extract_session_date()`
- `extract_agenda_section()`
- Imports: `from bs4 import BeautifulSoup`, `from PyPDF2 import PdfReader`, `import requests` (verificar que no lo usen otras funciones del fichero antes de eliminarlo)

### 4. Limpiar dependencias

- `requirements.txt`: eliminar `beautifulsoup4` y `lxml`
- `docker-compose.yml` y `docker-compose.prod.yml`: eliminar `PyPDF2` y `beautifulsoup4` de `_PIP_ADDITIONAL_REQUIREMENTS`

> **Precaución**: hacer `rg -l 'BeautifulSoup\|bs4' congress_videos/` antes de eliminar bs4 para confirmar que ningún otro módulo la usa.

### 5. Actualizar `save_chapters_to_db`

La task actualmente consume el XCom `session_date` de `extract_session_date`. El operador en `postgres_operators.py` (líneas 344-346) ya tiene fallback a `target_date` param — funciona sin cambios. `session_number` quedará `NULL` en BD.

Opcionalmente: eliminar la key `session_date` del dict `xcom_keys` del operador en el DAG para evitar confusión.

---

## Nuevo grafo (simplificado)

```
check_test_mode
    ├── create_test_video_data ─────────────────┐
    └── fetch_youtube_channel_videos            │
              ↓                                 │
      filter_plenary_sessions                   │
              ↓                                 │
      check_if_plenary_found                    │
         ├── no_plenary_sessions                │
         └── get_video_details ←────────────────┘
                  ↓
    ┌─────────────┴─────────────────┐
    ↓                               ↓
try_download_subtitles      download_video_from_youtube
    ↓
check_subtitles_available
    ├── split_srt_by_silence (subtítulos OK)
    └── extract_audio_from_youtube
              ↓
      transcribe_audio_with_whisper
              ↓
         merge_srt_files
              ↓
      split_srt_by_silence
              ↓
    summarize_silence_chunks
              ↓
    identify_interesting_chapters
              ↓
     merge_interesting_chapters
              ↓
      score_chapter_relevance
              ↓
       save_chapters_to_db
```

---

## Riesgos

- **`session_number` en BD quedará NULL**: si hay queries de reporting que dependan de ese campo, valorar alternativa (numeración incremental o API del congreso).
- **bs4 en otros módulos**: hacer grep global antes de eliminar del docker-compose.
- **Tests existentes**: buscar tests de las funciones eliminadas y borrarlos también.

---

## Esfuerzo estimado: S

Todo son eliminaciones puras — no hay lógica nueva. Un solo PR, un solo commit.

**Secuencia de aplicación:**
1. Eliminar funciones en `youtube_channel.py` + limpiar imports
2. Eliminar task definitions y edges del DAG
3. Limpiar `xcom_keys` de `save_chapters_to_db`
4. Limpiar `requirements.txt` y docker-compose

**Validar con:**
```bash
conda run -n airflow python congress_videos/youtube_channel_monitor_dag.py
```
