# Auditoría y plan de mejora: troceado de vídeos del Congreso con IA

## 1. Resumen ejecutivo

El sistema actual **NO trocea "sobre todo por silencios"** como se cree: los silencios son únicamente un *pre-troceo mecánico* para evitar mandar 4 horas de audio/SRT de golpe; la segmentación lógica real ya la hace un LLM (GPT-4o-mini) identificando capítulos por coherencia temática (`congress_videos/config/ai_prompts.py:195-357`). El problema de fondo es otro y más grave: **el LLM solo ve ~20 000 caracteres del SRT** (`congress_videos/modules/youtube/download.py:968`), es decir, alrededor del 1,5 % de una sesión de 4 h, por lo que decide "capítulos lógicos" a ciegas sobre el 98 % restante. A esto se suman un *pre-trim* truncado a 8 000 caracteres que sesga todos los clips hacia el inicio (`congress_videos/srt_helpers.py:153-170`), payloads de XCom de varios MB que cargan la metadata DB de Airflow, llamadas a OpenAI sin idempotencia, y cortes de ffmpeg con `-c copy` que se desplazan al keyframe más cercano (±2-5 s) (`congress_videos/modules/video_splitter.py:78-82`). Conclusión: cortar por silencios no es el villano; **el villano es la ceguera por truncamiento y la ausencia de map-reduce**. La mejora de mayor ROI es dejar que el LLM vea todo el SRT (cabe en su ventana de 128k) o, en su defecto, procesarlo por ventanas solapadas con consolidación jerárquica.

## 2. Cómo funciona hoy

Mapa de los niveles de troceado, de más bajo a más alto, con rutas reales:

| Nivel | Qué hace | Ruta |
|---|---|---|
| 1. Descarga por trozos de audio | Baja el audio en bloques temporales fijos para no saturar memoria | `utils/youtube_downloader.py:417-558` (`download_audio_in_chunks`); cálculo de bloques en `:490`, `:499`, `:516-524`, `:541-543` |
| 2. Pre-troceo por silencios (SRT) | Parte el SRT donde hay huecos ≥15 s entre subtítulos consecutivos — **mecánico, no semántico** | `congress_videos/modules/youtube/download.py:577-677` (`split_srt_by_silence`); detección de gaps en `utils/ai_chapter_analyzer.py:51-281` (`detect_silence_gaps` + `chunk_by_silence`), parse de timestamp en `:42-48` |
| 3. Resumen por trozo | Resume cada trozo de silencio con el LLM, en bucle **serie** | `congress_videos/modules/youtube/download.py:680-807` (`summarize_silence_chunks`), bucle en `:724-783` |
| 4. Identificación semántica de capítulos | El LLM agrupa por coherencia temática y devuelve capítulos lógicos — **aquí está la inteligencia real** | `congress_videos/modules/youtube/download.py:810-1056` (`identify_interesting_chapters`); prompt en `congress_videos/config/ai_prompts.py:195-357` (prioriza coherencia temática en `:199`); **truncado a 20k** en `:968`; fallback a capítulos vacíos en `:986-991` |
| 5. Merge + scoring de relevancia | Une capítulos y los puntúa; solo ordena por `start_time`, **sin deduplicar** | `congress_videos/modules/youtube/download.py:1059-1141` (`merge_interesting_chapters`); scoring en `congress_videos/modules/youtube/youtube_ai.py:301-531` (`score_chapters_relevance`); persistencia en `congress_videos/modules/postgres_operators.py:310-362` |
| 6. Corte físico con ffmpeg | Extrae cada capítulo del MP4 | `congress_videos/modules/video_splitter.py:36-134` / `:136-270`; `-c copy` (snap a keyframe) en `:78-82`; timeout 600 s en `:94`; conversión de tiempo frágil en `:31-33` |
| 7. Pre-trim IA del clip | Recorta una ventana fina dentro del capítulo buscando una frase | `congress_videos/srt_helpers.py:153-170` (`select_pretrim_window`) / `:173-263` (`_find_phrase_in_blocks`); `max_chars=8000`; matching en `:162`, `:167`; timestamps desde SRT en `:244-245`; orquestado en `congress_videos/reap_clip_preparer_dag.py:64-80`, `:121-210` |

Orquestación general en `congress_videos/youtube_channel_monitor_dag.py:342-497` (trigger rule en `:388`, dependencia en `:486`).

## 3. Puntos débiles detectados

### CRÍTICOS (impacto directo en "cortes lógicos" de vídeos >4 h)

1. **Truncamiento a 20 000 caracteres del SRT antes del LLM** (`download.py:968`). Una sesión de 4 h genera ~1,3 M de caracteres de SRT; el modelo decide la estructura de capítulos viendo ~1,5 %. *Este es el motivo real de que los cortes parezcan ilógicos*, no los silencios. GPT-4o-mini admite 128k tokens (~400k chars); un SRT de 4 h ≈ 75k tokens: **cabe entero**.
2. **Sin estrategia map-reduce / ventana deslizante.** Al no poder (o no querer) meter todo, no hay fallback que procese el documento por tramos y consolide. Es truncar o nada.
3. **Pre-trim truncado a 8 000 caracteres** (`srt_helpers.py`). La búsqueda de la frase solo mira el principio del capítulo, así que **los clips salen casi siempre del arranque** del tramo, no del momento relevante.
4. **Fallback a capítulos vacíos / mudo** (`download.py:986-991`). Si el LLM no devuelve capítulos válidos, el sistema continúa con una lista vacía en lugar de degradar a un troceo determinista por duración.
5. **Bug de rango/duración** (`download.py:911`): el cálculo de duración del capítulo puede producir rangos inconsistentes (start≥end) que ffmpeg corta como clip de 0 s o falla.
6. **Explosión de XCom** (varios MB de SRT/resúmenes viajando por XCom). Carga la metadata DB de Airflow y arriesga timeouts; los SRT deben ir por almacenamiento externo (S3/disco) con solo la ruta en XCom.
7. **Deriva por keyframe en ffmpeg** (`video_splitter.py:78-82`, `-c copy`). El corte salta al keyframe más cercano (±2-5 s). No rompe la "lógica" pero corta frases a media palabra.

### IMPORTANTES

8. **Resumen en bucle serie** (`download.py:724-783`): N llamadas secuenciales a OpenAI; en vídeos largos multiplica latencia y coste sin paralelizar.
9. **Llamadas a OpenAI sin idempotencia**: un retry de la task repite todo el coste; no hay cache por hash de input.
10. **Merge sin deduplicación** (`download.py:1059-1141`): capítulos solapados se concatenan, generando clips duplicados o redundantes.
11. **Conversión de tiempo frágil** (`video_splitter.py:31-33`, `srt_helpers.py`): parsing manual de `HH:MM:SS,mmm` propenso a fallos con formatos límite.

### MENORES

12. **Fecha/placeholder hardcodeados** en el prompt (`ai_prompts.py:431`, `:439`).
13. **Timeout fijo de 600 s** en ffmpeg (`video_splitter.py:94`): insuficiente para clips largos re-encodeados.
14. **Gap de silencio fijo a 15 s**: arbitrario; no se adapta al ritmo de la sesión.

## 4. Mejores opciones (research)

### SÓLIDAS para este caso

| Técnica | Cómo funciona | Herramientas | Pros / Contras | Integración |
|---|---|---|---|---|
| **Ampliar contexto del LLM (quitar el truncado a 20k)** | Mandar el SRT completo (cabe en 128k) y dejar que el modelo segmente sobre el 100 % | GPT-4o-mini nativo | + Arregla la causa raíz con un cambio mínimo · − Más tokens/coste por llamada | Sustituir `[:20000]` en `download.py:968` por el SRT completo (o por chunks de ~100k chars) |
| **Map-reduce con solape jerárquico** (estilo BooookScore) | Trocear por ventanas grandes solapadas, resumir/segmentar cada una, y consolidar capítulos en una segunda pasada | LangChain `map_reduce`/`refine`, lógica propia | + Escala a cualquier duración · − Más complejidad y 2x llamadas | Nuevo paso entre nivel 3 y 4; reusar `summarize_silence_chunks` como "map" |
| **Ventanas deslizantes con solape + self-consistency** (estilo SliSum) | Procesar tramos con solape para no perder fronteras entre ventanas | Lógica propia sobre el SRT | + Evita capítulos cortados en la juntura · − Hay que deduplicar fronteras | Refuerza el map-reduce anterior |
| **Idempotencia por hash de input** | Cachear respuesta del LLM por hash(SRT+prompt) | Tabla Postgres ya existente / Redis | + Retries gratis, ahorro de coste · − Hay que invalidar cache al cambiar prompt | Wrapper sobre las llamadas en `youtube_ai.py` |
| **SRT por almacenamiento externo, ruta en XCom** | Subir SRT/resúmenes a S3/disco y pasar solo la key | boto3 / fs local | + Mata la explosión de XCom · − Requiere bucket/limpieza | Cambiar payloads en `youtube_channel_monitor_dag.py` |

### DUDOSAS (nicho legítimo, pero no resuelven el problema central aquí)

| Técnica | Veredicto | Por qué |
|---|---|---|
| **TextTiling + embeddings BERT** (Solbiati μ-σ) | DUDOSA | Buen detector de fronteras temáticas, pero el LLM ya hace esto mejor con contexto completo; añade infra de embeddings sin ganancia clara |
| **TreeSeg (clustering divisivo)** | DUDOSA | Elegante para segmentación jerárquica, pero pesado para un pipeline Airflow que ya tiene LLM |
| **Kernel Change-Point Detection (`ruptures`)** | DUDOSA | Útil sobre señales de embeddings; complejidad de tuning alta para el retorno |
| **LangChain SemanticChunker / LlamaIndex SemanticSplitter** | DUDOSA | Pensado para RAG, no para capítulos de vídeo; corta por similitud de frases, no por estructura de sesión |
| **Diarización (pyannote, WhisperX, NeMo Sortformer)** | DUDOSA | El cambio de orador es buena señal de frontera en debates, pero mete GPU en la ruta crítica; reservar para v2 |
| **Chapter-Llama / PODTILE / ToC-LLM** | DUDOSA | SOTA en chaptering, pero requieren fine-tuning/modelos dedicados; sobredimensionado frente a "quitar el truncado" |
| **ARC-Chapter (OCR de rótulos)** | DUDOSA | El Congreso sí tiene rótulos de orador en pantalla → señal valiosa, pero es un proyecto en sí mismo |
| **Virality scoring / LLM-as-a-Judge (rúbrica 0-1-2)** | DUDOSA | Relevante para *seleccionar* clips, no para *trocear* lógicamente; útil en el scoring (nivel 5) |

### REFUTADAS para este caso

| Técnica | Por qué NO |
|---|---|
| **TextTiling clásico + C99** | Métodos pre-embeddings, peores que el LLM actual; regresión |
| **ClipsAI** | Su segmentador usa encoder monolingüe inglés; el contenido es español → resultados pobres |
| **Moment-DETR / QD-DETR / MS-DETR** | Recuperación de momentos sobre vídeo crudo; problema y modalidad equivocados, no troceo lógico de transcripción |
| **Técnicas de corte preciso ffmpeg (smart cut, force_key_frames, re-encode `-ss` antes de `-i`)** como solución a la "lógica" | Resuelven la deriva de keyframe (capa física), **no** la lógica de capítulos. Aplican al punto 7 de debilidades, no a la causa raíz |

## 5. Recomendación priorizada

| # | Mejora | Esfuerzo | Impacto | Archivos a tocar |
|---|---|---|---|---|
| 1 | Quitar truncado 20k → mandar SRT completo (o chunks de ~100k) | Bajo | **Muy alto** | `download.py:968` |
| 2 | Idempotencia por hash en llamadas OpenAI | Bajo | Alto | `youtube_ai.py:301-531`, tabla en `postgres_operators.py` |
| 3 | Ampliar/eliminar truncado 8k del pre-trim | Bajo | Alto | `srt_helpers.py:153-170` |
| 4 | Fallback determinista por duración si LLM devuelve vacío | Bajo | Alto | `download.py:986-991` |
| 5 | Arreglar bug de rango/duración (start≥end) | Bajo | Alto | `download.py:911` |
| 6 | Dedup de capítulos solapados en merge | Bajo | Medio | `download.py:1059-1141` |
| 7 | SRT a almacenamiento externo, ruta en XCom | Medio | Alto | `youtube_channel_monitor_dag.py:342-497` |
| 8 | Map-reduce con solape jerárquico (fallback para >100k chars) | Medio | **Muy alto** | nuevo módulo + `download.py:810-1056` |
| 9 | Paralelizar resúmenes (dynamic task mapping) | Medio | Medio | `download.py:680-807`, DAG |
| 10 | Corte ffmpeg con re-encode preciso en bordes (`-ss` precisión) | Medio | Medio | `video_splitter.py:36-134` |
| 11 | Parsing de tiempo robusto (lib única) | Bajo | Bajo | `video_splitter.py:31-33`, `srt_helpers.py` |
| 12 | Timeout ffmpeg adaptativo a duración | Bajo | Bajo | `video_splitter.py:94` |
| 13 | Gap de silencio adaptativo (percentil) | Bajo | Bajo | `ai_chapter_analyzer.py:51-281` |
| 14 | Quitar fecha/placeholder hardcodeados | Bajo | Bajo | `ai_prompts.py:431,439` |
| 15 | Self-consistency en fronteras de ventana | Medio | Medio | módulo map-reduce |
| 16 | (v2) Señal de cambio de orador vía diarización | Alto | Medio | nuevo paso pre-LLM |
| 17 | (v2) OCR de rótulos como señal de frontera | Alto | Medio | nuevo paso pre-LLM |

**Quick-wins (hacer ya):** #1, #2, #3, #4, #5, #6 — esfuerzo bajo, arreglan la causa raíz.
**Apuestas a medio plazo:** #7, #8, #9, #10.
**Apuestas a largo (v2, no en ruta crítica):** #16, #17.

## 6. Arquitectura propuesta

Mantiene el **núcleo LLM** (que ya funciona) y le quita las vendas. NO mete GPU, embeddings ni diarización en la ruta crítica.

```
[YouTube] → descarga audio por trozos (igual que hoy)
     │
     ▼
[Transcripción → SRT completo] ──► SUBIR a S3/disco; XCom solo lleva la key
     │
     ▼
┌─────────────────────────────────────────────────────────┐
│  SEGMENTACIÓN SEMÁNTICA (LLM)                            │
│                                                          │
│  ¿SRT ≤ ~100k chars?                                     │
│    SÍ → una sola llamada con el SRT COMPLETO (#1)        │
│    NO → MAP-REDUCE con ventanas solapadas (#8):          │
│           map:   segmentar cada ventana (paralelo, #9)   │
│           reduce: consolidar capítulos + self-consist.   │
│                   en fronteras (#15)                     │
│                                                          │
│  Cache idempotente por hash(SRT+prompt) (#2)            │
│  Fallback determinista por duración si vacío (#4)       │
└─────────────────────────────────────────────────────────┘
     │
     ▼
[Merge + DEDUP de solapes (#6)] → [Scoring de relevancia (igual que hoy)]
     │
     ▼
[Corte ffmpeg con bordes precisos (#10) + rango validado (#5)]
     │
     ▼
[Pre-trim IA sin truncar a 8k (#3)] → clips lógicos finales
```

**Qué reemplaza:** el truncado a 20k (nivel 4) por SRT completo o map-reduce; el pre-trim ciego (nivel 7) por búsqueda sobre todo el capítulo. **Qué complementa:** el pre-troceo por silencios (nivel 2) sigue, pero ahora solo como ayuda de paralelización, no como límite de lo que ve el LLM. La inteligencia semántica sigue en el LLM; lo que cambia es que **ahora ve el material entero**.

## 7. Fuentes

- BooookScore — coherencia de resúmenes de libros (map-reduce vs incremental): https://arxiv.org/abs/2310.00785
- SliSum — sliding window + self-consistency para resumen fiel: https://arxiv.org/abs/2407.15681
- TextTiling (Hearst, 1997): https://aclanthology.org/J97-1003/
- C99 (Choi, 2000): https://aclanthology.org/A00-2004/
- Solbiati et al. — Unsupervised topic segmentation con BERT embeddings (μ-σ): https://arxiv.org/abs/2106.12978
- TreeSeg — segmentación jerárquica por clustering divisivo: https://arxiv.org/abs/2407.12028
- `ruptures` — change point detection en Python: https://centre-borelli.github.io/ruptures-docs/
- LangChain SemanticChunker: https://python.langchain.com/docs/how_to/semantic-chunker/
- LlamaIndex SemanticSplitterNodeParser: https://docs.llamaindex.ai/en/stable/module_guides/loading/node_parsers/modules/
- pyannote.audio — speaker diarization: https://github.com/pyannote/pyannote-audio
- WhisperX — alineación + diarización: https://github.com/m-bain/whisperX
- NVIDIA NeMo Sortformer — diarización streaming: https://github.com/NVIDIA/NeMo
- Chapter-Llama — chaptering de vídeos largos con LLM: https://arxiv.org/abs/2504.00072
- PODTILE — chaptering de podcasts: https://arxiv.org/abs/2410.16148
- ToC-LLM — generación de tabla de contenidos con LLM: https://arxiv.org/abs/2406.13393
- ARC-Chapter — chaptering con señales OCR de rótulos: https://arxiv.org/abs/2407.14642
- Moment-DETR / QVHighlights — moment retrieval & highlight detection: https://arxiv.org/abs/2107.09609
- QD-DETR: https://arxiv.org/abs/2303.13874
- ClipsAI — librería de clipping: https://github.com/ClipsAI/clipsai
- OpusClip / virality scoring (referencia de producto): https://www.opus.pro/
- FFmpeg seeking & keyframe accuracy (corte preciso): https://trac.ffmpeg.org/wiki/Seeking
- FFmpeg `force_key_frames` / segment muxer: https://ffmpeg.org/ffmpeg-formats.html#segment_002c-stream_005fsegment_002c-ssegment
