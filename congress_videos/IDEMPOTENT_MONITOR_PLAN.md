# Plan: Idempotencia en `congress_youtube_channel_monitor`

> Generado por workflow de planificación (`/workflows`) + revisión de arquitecto.
> Enfoque elegido: **A** — task dedicada `filter_unprocessed_videos` en el camino de producción.

## Enfoque elegido

Approach A — una task `PythonOperator` `filter_unprocessed_videos` (t2b) insertada **solo en el
camino de PRODUCCIÓN**, entre `filter_plenary_sessions` (t2) y `check_if_plenary_found` (t2a).
Hace pull del XCom `plenary_videos`, consulta `{schema}.youtube_source_videos` por los `video_id`
con `is_processed = TRUE`, los descarta, recalcula `total_matches` y vuelve a hacer push bajo la
MISMA clave `plenary_videos`. El branch existente enruta a `no_plenary_sessions` gratis cuando no
queda nada. Política de error de BD: **FAIL-CLOSED** (raise → run falla → `retries=2`).
Approaches B (fold en el branch) y C-capas-2/3 (UNIQUE+ON CONFLICT, skip en disco) se rechazan/difieren.

## Proposal

El DAG re-descarga, re-transcribe (Whisper) y re-puntúa (IA) vídeos en cada ejecución, aunque el
`video_id` ya se procesara antes. Desperdicia cuota de YouTube API, ancho de banda, tiempo de
transcripción y tokens de IA, y — como el INSERT en `video_chapters` NO tiene conflict handling —
duplica filas de capítulos al reprocesar. Objetivo del usuario: "saber qué vídeos ya tengo y no
bajarlos", con la comprobación ANTES de cualquier descarga/transcripción.

El estado ya existe: `save_youtube_chapters_to_db` hace UPSERT de `{schema}.youtube_source_videos`
con `is_processed = TRUE` solo al FINAL de un pipeline exitoso. Un filtro pre-descarga que descarta
los `video_id` con `is_processed = TRUE` corta el pipeline caro vía el branch existente
`check_if_plenary_found` (que enruta según `total_matches > 0`).

### Invariante de correctitud (NO TOCAR)
`is_processed = TRUE` se escribe SOLO al final de un pipeline totalmente exitoso. Un run que falló
parcialmente deja `is_processed = FALSE`, así que el filtro NO lo salta y se reprocesa correctamente.

### Política de error de BD
FAIL-CLOSED: ante error de BD el filtro RAISE → la task falla → `retries=2`. No se ha descargado
nada aún, así que fallar es barato. Fail-open se rechaza porque `video_chapters` aún no tiene
ON CONFLICT (diferido).

## Spec — Requisitos

- **R1**. El DAG NO debe descargar/extraer audio/transcribir/puntuar ningún `video_id` con
  `is_processed = TRUE` en `{schema}.youtube_source_videos`, en el camino de producción.
- **R2**. La comprobación se ejecuta ANTES de cualquier task de descarga/transcripción/IA.
- **R3**. Lookup parametrizado e indexado: `SELECT video_id FROM {schema}.youtube_source_videos
  WHERE video_id = ANY(%s) AND is_processed = TRUE`. Valores como params, nunca interpolados.
  Nombre de tabla vía `pg_conn.get_qualified_table('youtube_source_videos')`.
- **R4**. Preservar el contrato del XCom `plenary_videos` `{'total_matches', 'videos', 'target_date'}`
  (y demás keys top-level), re-push bajo la MISMA clave. Cero cambios aguas abajo.
- **R5**. Si TODOS están procesados, `total_matches = 0` → branch a `no_plenary_sessions`.
- **R6**. Un vídeo con `is_processed = FALSE` o sin fila NO se filtra (retry tras fallo parcial).
- **R7**. El camino de TEST no se ve afectado — el vídeo de test siempre se (re)procesa.
- **R8**. FAIL-CLOSED: cualquier excepción de BD se propaga (la task falla, aplican retries).

### Escenarios
- **S1** (skip parcial): [A,B], DB={A} → keep [B], total_matches=1.
- **S2** (todos procesados): [A,B], DB={A,B} → keep [], total_matches=0 → `no_plenary_sessions`.
- **S3** (ninguno): [A,B], DB={} → keep [A,B], total_matches=2 (sin cambios).
- **S4** (retry fallo parcial): fila is_processed=FALSE o ausente → query devuelve {} → se reprocesa.
- **S5** (input vacío/None): sin ejecutar query; devuelve dict con forma `{'total_matches':0,'videos':[]}`.
- **S6** (bypass test-mode): `t0_branch >> t0_test >> [t3a,t3b]` nunca pasa por t2b.
- **S7** (outage BD): raise propaga → t2b falla → retries=2 → run falla pronto (sin descargas).

## Design

### File 1 — `congress_videos/modules/database.py` (nuevo método en `CongressionalVideoDB`)
```python
def get_processed_video_ids(self, video_ids: list[str]) -> set[str]:
    """
    Return the subset of video_ids already fully processed
    (is_processed = TRUE) in youtube_source_videos.

    Cheap pre-download idempotency check. Empty input -> empty set
    (no query executed). Read-only; raises on DB error (fail-closed).
    """
    if not video_ids:
        return set()

    youtube_videos_table = self.pg_conn.get_qualified_table('youtube_source_videos')

    with self.pg_conn.get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT video_id
                FROM {youtube_videos_table}
                WHERE video_id = ANY(%s) AND is_processed = TRUE
                """,
                (video_ids,),
            )
            return {row['video_id'] for row in cur.fetchall()}
```
Notas: `ANY(%s)` + `list` es el idiom correcto psycopg2 (NO `IN %s`). Guard de empty nunca toca SQL.
Type hints builtin (`list[str]`/`set[str]`) per py312. Lookup por PK → indexado barato.

### File 2 — `congress_videos/modules/youtube/youtube_channel.py` (nueva función)
```python
def filter_unprocessed_videos(plenary_videos: dict) -> dict:
    """
    Drop videos whose video_id is already fully processed in the DB.

    Input/output shape: {'total_matches': int, 'videos': list, 'target_date': str}
    Preserves 'target_date' and any other top-level keys; rewrites only
    'videos' and recomputes 'total_matches'.

    FAIL-CLOSED: on DB error this raises (does NOT treat all as unprocessed).
    PRODUCTION path only (test mode never reaches this task).
    """
    import logging
    if not plenary_videos or not plenary_videos.get('videos'):
        return plenary_videos or {'total_matches': 0, 'videos': []}

    from congress_videos.modules.database import CongressionalVideoDB

    videos = plenary_videos['videos']
    video_ids = [v['video_id'] for v in videos if v.get('video_id')]

    db = CongressionalVideoDB()
    already_processed = db.get_processed_video_ids(video_ids)

    kept = [v for v in videos if v.get('video_id') not in already_processed]
    skipped = len(videos) - len(kept)
    logging.info(
        "Idempotency filter: %d candidate(s), %d already processed, %d to process. Skipped ids: %s",
        len(videos), skipped, len(kept), sorted(already_processed),
    )

    result = dict(plenary_videos)            # preserve target_date + any extra keys
    result['videos'] = kept
    result['total_matches'] = len(kept)
    return result
```
Import de DB DENTRO del cuerpo (módulo DB-free en import). Tests parchean
`congress_videos.modules.database.CongressionalVideoDB` (ruta SOURCE). SIN try/except (fail-closed).

### File 3 — `congress_videos/modules/youtube/__init__.py` (registro LOAD-BEARING)
`__getattr__` es el único punto de resolución. Extender la rama `youtube_channel`:
- Añadir `'filter_unprocessed_videos'` a la lista `if name in [...]`.
- Añadir `filter_unprocessed_videos,` a la tupla `from .youtube_channel import (...)`.
- (Recomendado) Añadir a `__all__`.
- **GOTCHA**: omitir el edit de `__getattr__` → `AttributeError` en RUNTIME (no en parse). `__all__` solo es cosmético.

### File 4 — `congress_videos/youtube_channel_monitor_dag.py` (nueva task + wiring)
```python
# Step 2b: Idempotency filter - drop videos already fully processed in DB.
# Runs BEFORE any download/transcription. PRODUCTION path only
# (test path goes t0_test >> [t3a, t3b] and never reaches this task).
t2b = PythonOperator(
    task_id='filter_unprocessed_videos',
    python_callable=lambda ti: xcom_task(
        ti,
        lambda: yt_channel.filter_unprocessed_videos(
            ti.xcom_pull(key='plenary_videos')
        ),
        'plenary_videos',          # overwrite same key
    ),
)
```
- trigger_rule: DEFAULT (`all_success`). Single upstream (t2). NO usar `none_failed_min_one_success`.
- Wiring (solo esa línea): `t1 >> t2 >> t2a` → `t1 >> t2 >> t2b >> t2a`.
- SIN CAMBIOS: `t0_branch >> [t0_test, t1]`; `t0_test >> [t3a, t3b]`; `t2a >> t_end`; `t2a >> [t3a, t3b]`.

### Tests
- **T1** `tests/congress_videos/modules/test_database.py` → clase `TestGetProcessedVideoIds`
  (empty→set() sin execute; SQL con `youtube_source_videos`/`is_processed = TRUE`/`ANY(%s)`,
  `params == (video_ids,)`; fetchall vacío → set vacío).
- **T2** `tests/congress_videos/modules/youtube/test_youtube_channel.py` → clase
  `TestFilterUnprocessedVideos` (parchear `congress_videos.modules.database.CongressionalVideoDB`;
  drop/keep + preserva target_date; empty/None sin DB; error de BD propaga).
- **T3** `tests/congress_videos/test_youtube_channel_monitor_dag.py` (greenfield, espejo de
  `test_reap_processor_dag.py`): dag_id, schedule `'0 22 * * *'`, presencia de la task, topología
  entre t2 y t2a, y ausencia del camino de test.

## Tareas (orden de implementación)
1. `database.py`: añadir `get_processed_video_ids`.
2. `youtube_channel.py`: añadir `filter_unprocessed_videos` (import DB en cuerpo, fail-closed).
3. `youtube/__init__.py`: registrar en `__getattr__` (+ `__all__`). **Load-bearing.**
4. DAG: definir `t2b` tras el bloque de t2a (~línea 145), trigger_rule por defecto.
5. DAG: cambiar línea 430 `t1 >> t2 >> t2a` → `t1 >> t2 >> t2b >> t2a`. Resto byte-a-byte igual.
6. T1 tests de `get_processed_video_ids`.
7. T2 tests de `filter_unprocessed_videos`.
8. T3 test greenfield de carga del DAG.
9. Correr: `conda run -n airflow python -m pytest tests/congress_videos/modules/test_database.py tests/congress_videos/modules/youtube/test_youtube_channel.py tests/congress_videos/test_youtube_channel_monitor_dag.py -q`. NO build.

## Riesgos
- Olvidar el registro en `__getattr__` → `AttributeError` en RUNTIME. Mitiga: T2/T3 ejercitan el call path.
- t2b mete dependencia de BD ANTES de descargas. Outage persistente falla el run pronto. Mitiga: retries=2; fail-closed intencionado.
- Bypass test-mode es TOPOLÓGICO. Refactor futuro podría filtrarlo. Mitiga: T3 lo fija.
- Parchear ruta equivocada en T2: parchear `congress_videos.modules.database.CongressionalVideoDB` (source).
- NO arregla el INSERT sin ON CONFLICT en `video_chapters`. Diferido a otro PR.
- Si alguien mueve el write de `is_processed=TRUE` antes, se estancarían vídeos. Mitiga: comentario + test S4/R6.

## Diferido (fuera de scope)
- UNIQUE constraint en `video_chapters (video_id, start_time, end_time)` + ON CONFLICT (migración de schema, su propio PR).
- Skip por existencia de fichero en disco (solo si las descargas van a storage persistente compartido).
- Enrutar el filtro por `PostgreSQLOperator` (rechazado; PythonOperator + `xcom_task` mantiene la lógica testeable).
