# Pipeline end-to-end: de la sesión plenaria al vídeo publicado

Vista por fases del flujo completo de `congress_videos`, desde que aparece una
sesión nueva en el canal del Congreso hasta que sus fragmentos están publicados
y medidos. El detalle tarea a tarea de cada DAG está en [DAGS.md](DAGS.md).

> **Orden objetivo (issue #117):** el orden conceptual correcto es refinar por
> orador **antes** de subir (la unidad de publicación debería ser el turno de
> orador, no el capítulo temático). Hoy la Fase 2 es una cadena on-demand
> paralela y el uploader consume capítulos sin refinar; la reordenación está
> pendiente de diseño en #117.

```
Fase 0 (ingesta+análisis)  →  Fase 1 (subida de capítulos)  →  Fase 4 (analítica)
        │                            ▲
        └── Fase 2 (refinamiento por orador, on-demand — futuro: delante de Fase 1)
        └── Fase 3 (shorts Reap)
```

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

## Fase 1 — Subida de capítulos · `congress_youtube_chapter_uploader`

Publica los capítulos más relevantes como vídeos independientes.

1. **Guardas**: cuota diaria de subidas y guarda de staleness (evita runs
   obsoletos re-planificados tras un `git_sync`).
2. **Selección**: lee la vista `uploadable_chapters` (relevancia mínima,
   orador resuelto).
3. **Metadata + miniatura**: título formato noticia anclado a `key_speakers` y
   descripción por IA; dispara `generic_thumbnail_generator` (Pikzels con
   dirección de arte, zona segura, arquetipo dramático y cita lapidaria).
4. **Corte**: extrae el capítulo del vídeo fuente con ffmpeg (frame-accurate,
   re-encode consciente del códec).
5. **Publicación**: sube al canal, marca el capítulo como subido, propaga el
   `video_id` a la miniatura y registra fallos.

## Fase 2 — Refinamiento por orador (on-demand · epic #16)

Cadena que convierte capítulos temáticos en turnos de orador listos para
publicar. Los tres DAGs se disparan vía API (`schedule=None`).

1. **`speaker_turns`** (#86): diarización pyannote vía el sidecar
   `diarize-api` → turnos de orador nombrados dentro de cada capítulo
   (tabla `speaker_turns`).
2. **`trim_proposals`** (#87): detección de silencios y aplausos vía el sidecar
   `yamnet-api` → propuestas de recorte **no destructivas** que requieren
   aprobación de operador (la voz siempre prevalece; cero cortes automáticos).
3. **`speaker_turn_videos`** (#88): materializa un MP4 por turno (o grupo de
   turnos cortos consecutivos) ejecutando solo los cortes aprobados.

Los vídeos materializados **no se suben automáticamente** todavía; la
integración con la fase de subida es el objeto de #117.

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
