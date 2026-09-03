# Pipeline end-to-end: de la sesión plenaria al vídeo publicado

Vista por fases del flujo completo de `congress_videos`, desde que aparece una
sesión nueva en el canal del Congreso hasta que sus fragmentos están publicados
y medidos. El detalle tarea a tarea de cada DAG está en [DAGS.md](DAGS.md).

> **Unidad de publicación (issues #117 y #171):** la unidad publicada es el
> **turno de orador**, no el capítulo temático. La reordenación se diseñó en
> #117 y se completó en #171: el uploader dejó de seleccionar capítulos y hoy
> lee la vista `uploadable_turns`. La Fase 2 dejó de ser una rama paralela y
> pasó a ser un paso previo obligatorio de la Fase 1: un turno solo es
> publicable si su MP4 está materializado y preparado.

```
Fase 0 (ingesta+análisis)  →  Fase 2 (refinamiento por orador)  →  Fase 1 (subida de turnos)  →  Fase 4 (analítica)
        │
        └── Fase 3 (shorts Reap)
```

Los números de fase son históricos: la Fase 2 se numeró cuando iba después de
la Fase 1. Se mantienen para no romper las referencias externas.

---

## Fase 0 — Ingesta y análisis · `congress_youtube_channel_monitor`

Detecta y analiza la sesión; no publica nada.

1. **Detección**: busca streams "Sesión Plenaria (original)" terminados en el
   canal oficial y descarta los ya procesados.
2. **Obtención**: detalles y descripción del vídeo, intento de subtítulos de
   YouTube, descarga del vídeo completo, verificación de integridad del source
   (los sources AV1 exigen re-encode al cortar) y extracción de audio.
3. **Transcripción**: `whisper-api` (sidecar HTTP persistente) y fusión de SRT.
4. **Contexto de sesión**: enlaces de la descripción, nota de prensa, agenda /
   orden del día y fecha de la sesión.
5. **Análisis IA**: troceo del SRT por silencios → resumen por chunk → agregado
   → identificación de capítulos interesantes → merge → puntuación de
   relevancia (0-5) → recorte de silencios de borde → normalización de
   oradores → persistencia en `video_chapters`.

## Fase 1 — Subida de turnos · `congress_youtube_chapter_uploader`

Publica un turno de orador al día como vídeo independiente (19:00 UTC,
`DAILY_LONG_FORM_UPLOAD_LIMIT = 1`). El DAG conserva el nombre `chapter` por
compatibilidad; ver "Nomenclatura" en [DAGS.md](DAGS.md).

1. **Guardas**: cuota diaria de subidas y guarda de staleness (evita runs
   obsoletos re-planificados tras un `git_sync`).
2. **Selección**: lee la vista `uploadable_turns` con `LIMIT 1` y sin ordenar
   por fuera, así que el orden interno de la vista (migración 044) decide qué
   turno se publica. La selección por capítulo, que leía `uploadable_chapters`,
   se retiró en #171; esa vista sigue viva pero la consumen otros DAGs.
3. **Metadata + miniatura**: título formato noticia anclado a `key_speakers` y
   descripción por IA; dispara `generic_thumbnail_generator` (Pikzels con
   dirección de arte, zona segura, arquetipo dramático y cita lapidaria).
4. **Corte**: no hay corte. El MP4 del turno ya lo materializó la Fase 2, así
   que `extract_chapter_videos` solo reutiliza su `output_path`. La rama ffmpeg
   sigue en el código pero no se alcanza con `item_type="turn"`.
5. **Publicación**: sube al canal, marca el turno como subido, propaga el
   `video_id` a la miniatura y registra fallos.

## Fase 2 — Refinamiento por orador (epic #16)

Cadena que convierte capítulos temáticos en turnos de orador listos para
publicar. Es el paso previo obligatorio de la Fase 1: sin un MP4 materializado
y marcado como preparado, el turno no entra en `uploadable_turns`. Los DAGs de
la cadena se disparan vía API (`schedule=None`).

1. **`speaker_turns`** (#86): diarización pyannote vía el sidecar
   `diarize-api` → turnos de orador nombrados dentro de cada capítulo
   (tabla `speaker_turns`).
2. **`trim_proposals`** (#87): detección de silencios y aplausos vía el sidecar
   `yamnet-api` → propuestas de recorte **no destructivas** que requieren
   aprobación de operador (la voz siempre prevalece; cero cortes automáticos).
3. **`speaker_turn_videos`** (#88): materializa un MP4 por turno (o grupo de
   turnos cortos consecutivos) ejecutando solo los cortes aprobados.
4. **`speaker_turn_prepare`** (#146): genera los sidecars (`subtitles.srt`),
   valida el MP4 con un decode de ffmpeg y solo entonces marca `prepared_at`.
   Es la puerta de entrada a `uploadable_turns`.

Los vídeos materializados sí se suben automáticamente desde #171: son la única
fuente de la Fase 1. El paso que los habilita es `speaker_turn_prepare`, que
escribe los sidecars y marca `prepared_at`; hasta entonces el turno no aparece
en `uploadable_turns`.

## Fase 3 — Shorts · pipeline Reap

1. **`reap_clip_preparer`** (diario 15:00 UTC): selecciona capítulos elegibles,
   pre-recorta clips largos con IA + contexto SRT y los encola.
2. **`reap_processor`** (14:30 y 17:30 UTC): reclama exactamente un clip por
   run y lo procesa vía Reap a formato short.
3. **`reap_shorts_uploader`** (5 veces al día): sube un short por run con
   título generado por IA; tras 3 fallos el clip se marca abandonado.

## Fase 4 — Post-subida · `video_analytics`

`@hourly`: recoge snapshots de YouTube Analytics en los checkpoints 24 h /
48 h / 7 d / 30 d / 90 d por vídeo subido (`video_analytics_snapshots`).
Solo colecta; las acciones automáticas (cambio de miniatura/título por CTR
bajo) están diferidas a #102.

---

## Infraestructura transversal

| Pieza | Rol |
|-------|-----|
| `git_sync_dag` → `run_migrations` | Sincroniza el repo a `origin/dev` en el Airflow del NAS y aplica migraciones SQL |
| `congress_participants_sync` (semanal) | Roster de diputados con fotos, base de la resolución de oradores |
| `generic_thumbnail_generator` / `generic_video_editor` | Servicios on-demand de miniaturas y rótulos reutilizables entre DAGs |
| Sidecars ML (`whisper-api`, `diarize-api`, `yamnet-api`) | Modelos servidos por HTTP en la red compose; `diarize-api` y `yamnet-api` con modo dormido (carga perezosa + idle-exit, RAM burst-only) — ver [ops/sidecar-model-apis.md](ops/sidecar-model-apis.md) |
