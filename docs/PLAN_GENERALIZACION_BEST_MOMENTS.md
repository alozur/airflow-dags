# Plan de Generalización: Detección de Mejores Momentos en Videos Largos

> **Autor:** Alonso Zurera & Buffy (AI Assistant)
> **Fecha:** 2026-07-23
> **Estado:** ✅ Plan consolidado (v2) — listo para Fase 1a

---

## 1. Resumen Ejecutivo

Actualmente el proyecto tiene un pipeline altamente especializado para **sesiones plenarias del Congreso de los Diputados de España** (`congress_youtube_channel_monitor` DAG). El objetivo es **generalizar** el núcleo del pipeline —la detección de los mejores momentos en videos largos— para que sea reutilizable con **cualquier tipo de video largo**, en cualquier idioma, con cualquier dominio temático.

---

## 2. Lo que YA tenemos (inventario de assets reutilizables)

### 2.1 Pipeline core (el "corazón" de la detección)

El pipeline actual, que vive en `congress_videos/modules/youtube/download.py`, es:

```
[Vídeo largo + Transcripción/SRT]
    │
    ▼
① split_srt_by_silence()       → divide SRT en chunks por pausas de silencio
    │                              (detección adaptativa de gaps)
    ▼
② summarize_silence_chunks()    → cada chunk → GPT-4o-mini extrae:
    │                              speakers, topics, timeline, summary
    ▼
③ identify_interesting_chapters() → AI decide si dividir el chunk en
    │                               sub-capítulos (con map-reduce para SRTs largos)
    ▼
④ merge_interesting_chapters()  → consolida y deduplica capítulos
    │
    ▼
⑤ score_chapters_relevance()    → puntúa cada capítulo (0-5)
    │                              con 3 criterios: speaker, topic, interés público
    ▼
⑥ [Guardar en DB / subir a YouTube / crear Shorts]
```

### 2.2 Módulos y utilidades reutilizables

| Módulo | Qué hace | Acoplamiento |
|---|---|---|
| `utils/ai_chapter_analyzer.py` | `detect_silence_gaps()`, `chunk_by_silence()` | **Bajo** — genérico, opera sobre texto SRT |
| `congress_videos/modules/youtube/map_reduce_chapters.py` | `window_srt()`, `map_chapters()`, `reduce_chapters()` | **Bajo** — genérico, recibe `identify_fn` inyectable |
| `congress_videos/srt_helpers.py` | `parse_srt_to_text()`, `select_pretrim_window()` | **Medio** — pre-trim usa prompt fijo |
| `congress_videos/modules/vad_helpers.py` | VAD para trim de silencio en bordes | **Bajo** — genérico |
| `congress_videos/modules/video_splitter.py` | `split_video_chapter()`, `build_ffmpeg_cut_cmd()` | **Bajo** — genérico |
| `utils/ai_helpers.py` | `generate_json_completion()` | **Bajo** — wrapper genérico de OpenAI |
| `utils/llm_cache.py` | `cached_json_completion()` | **Bajo** — cache idempotente genérico |
| `utils/whisper_helpers.py` | `transcribe_audio_file()`, `merge_srt_files()` | **Bajo** — genérico |
| `utils/time_utils.py` | `parse_timestamp()`, `format_timestamp()` | **Bajo** — utilidades puras |
| `congress_videos/modules/youtube/youtube_ai.py` | `score_chapters_relevance()`, metadata generation | **Alto** — específico del Congreso |
| `congress_videos/config/ai_prompts.py` | **TODOS los prompts de IA** | **Alto** — 100% específico |

---

## 3. Diagnóstico: ¿Qué está acoplado al Congreso?

### 3.1 Prompts de IA (el acoplamiento más fuerte)

**Archivo:** `congress_videos/config/ai_prompts.py`

Todos los prompts asumen: debates parlamentarios españoles, oradores políticos, temas de actualidad española, idioma español.

**Prompts a generalizar:**

| Prompt | Estrategia |
|---|---|
| `CHAPTER_IDENTIFICATION_SYSTEM_PROMPT` | → Parametrizar con `{content_type}` y `{language}` |
| `CHAPTER_IDENTIFICATION_USER_PROMPT_TEMPLATE` | → Parametrizar con ejemplos del dominio |
| `CHAPTER_RELEVANCE_SCORING_SYSTEM_PROMPT` | → Criterios configurables desde YAML/JSON |
| `CHAPTER_RELEVANCE_SCORING_USER_PROMPT_TEMPLATE` | → Parametrizar con `{current_date}`, `{domain_context}` |
| `CHUNK_SUMMARY_SYSTEM_PROMPT` | → Generalizar |
| `CHUNK_SUMMARY_USER_PROMPT_TEMPLATE` | → Ya es relativamente genérico |
| `SHORTS_METADATA_*` | → Fuera del scope (no generalizamos Shorts) |

### 3.2 Lógica de scoring

El scoring 0-5 actual es inherentemente político: speaker_relevance (Sánchez/Feijóo), topic_relevance (crisis/escaándalos), public_interest (confrontación). Para otros dominios los criterios son completamente distintos.

---

## 4. TODAS las decisiones tomadas ✅

| # | Decisión | Elección |
|---|---|---|
| 1 | Dominios adicionales | Conferencias tech, podcasts (contenido basado en diálogo) |
| 2 | ¿Generalizar Shorts/Reap también? | **NO** — solo el pipeline de detección de capítulos |
| 3 | Compatibilidad hacia atrás | **SÍ** — el Congreso debe funcionar exactamente igual |
| 4 | Escala de scoring | **0-5 fija** para todos los dominios |
| 5 | Input del motor | **`video_path` + `srt_path` opcional** |
| 6 | Output del motor | **Devuelve datos + guarda en DB** (si se pasa conexión) |
| 7 | Modelos de IA | **Solo OpenAI (GPT-4o-mini)** por ahora |
| 8 | Formato de DomainConfig | **YAML / JSON** por dominio |
| 9 | API pública | **Función `pipeline()`** — simple, testeable, sin Airflow |
| 10 | Manejo de errores | **Best-effort** — fallbacks, nunca crashea |
| 11 | Transcripción | **Automática con Whisper** si no se pasa SRT |
| 12 | Nombre del módulo | `utils/best_moments/` |

---

## 5. Arquitectura final propuesta

### 5.1 Testing strategy: LLM inyectable

El motor genérico debe ser **testeable sin llamadas reales a OpenAI**. Para ello,
el pipeline recibe un `llm_client` inyectable (Protocol) que los tests pueden
reemplazar con un mock. Esto sigue el mismo patrón que `map_reduce_chapters` ya
usa con `identify_fn`.

```python
class LLMClient(Protocol):
    """Interface for LLM calls — swap for mocks in tests."""
    def json_completion(
        self, system_prompt: str, user_prompt: str,
        *, model: str, temperature: float, max_tokens: int
    ) -> dict: ...
```

En producción se inyecta `OpenAIClient` (wrapper de `generate_json_completion`).
En tests se inyecta un mock que devuelve respuestas predefinidas.

### 5.2 API pública

```python
# utils/best_moments/pipeline.py

def pipeline(
    video_path: str,
    domain_config_path: str,        # path a YAML/JSON
    *,
    save_callback: Callable[[PipelineResult], None] | None = None,
    srt_path: str | None = None,
    llm_client: LLMClient | None = None,  # inyectable para tests
) -> PipelineResult:
    """
    Detecta los mejores momentos en un video largo.

    Args:
        video_path: Ruta al archivo de video (.mp4, .mkv, .webm).
        domain_config_path: Ruta al archivo YAML/JSON de configuración del dominio.
        save_callback: Si se proporciona, se llama con el PipelineResult para
            persistir los capítulos. El motor NUNCA toca la DB directamente.
        srt_path: Ruta opcional a SRT existente. Si no se da, se transcribe con Whisper.
        llm_client: Cliente LLM inyectable. Si es None, se usa OpenAIClient por defecto.
            Los tests inyectan un mock para evitar llamadas reales a OpenAI.

    Returns:
        PipelineResult con scored_chapters y metadata.

    Ejemplo desde el DAG del Congreso:
        result = pipeline(
            video_path,
            "congress_videos/config/domain_config.yaml",
            save_callback=lambda r: db.save_youtube_chapters_to_db(
                r.to_scored_chapters_dict()
            ),
        )

    Ejemplo en un test:
        mock_llm = MockLLMClient({"interesting_chapters": [...]})
        result = pipeline(video_path, config, llm_client=mock_llm)
        assert result.total_chapters == 3
    """
```

> **Decisión de diseño:** Usamos un **callback** para persistencia + **LLM inyectable**
> para testing. El motor no sabe nada de PostgreSQL ni requiere OpenAI real para ser testeado.

### 5.2 Estructura de archivos

```
utils/
  best_moments/                          ← NUEVO: motor genérico
    __init__.py
    pipeline.py                          ← orquesta el pipeline ①→⑤
    config.py                            ← carga DomainConfig desde YAML/JSON
    scoring.py                           ← scoring 0-5 con criterios configurables
    prompts.py                           ← prompts parametrizables con {placeholders}
    types.py                             ← dataclasses: DomainConfig, PipelineResult, Chapter
    map_reduce.py                        ← movido desde congress_videos/modules/youtube/
    chunking.py                          ← split por silencio (movido de download.py)
    temp_files.py                        ← gestión de archivos temporales SRT
  vad_helpers.py                         ← movido desde congress_videos/modules/
  ai_chapter_analyzer.py                 ← se queda, ya es genérico

congress_videos/                         ← instancia de dominio
  config/
    domain_config.yaml                   ← configuración YAML del Congreso
    ai_prompts.py                        ← prompts específicos del Congreso (se mantienen)
  modules/
    youtube/
      download.py                        ← SOLO descarga de video/audio (yt-dlp)
      youtube_ai.py                      ← metadata YouTube (titles, descriptions, chapters block)
    srt_helpers.py                       ← pre-trim para Shorts (se queda en Congreso)

docs/
  PLAN_GENERALIZACION_BEST_MOMENTS.md    ← este documento
```

### 5.3 DomainConfig (YAML)

```yaml
# congress_videos/config/domain_config.yaml
domain_name: "congress"
language: "es"
language_name: "español"
content_type: "sesiones parlamentarias españolas"

transcription:
  whisper_model_size: "tiny"
  prefer_youtube_subtitles: true

chunking:
  min_silence_seconds: 15
  min_chunk_duration_minutes: 10
  max_chunk_duration_minutes: 20
  use_adaptive_silence: true
  adaptive_percentile: 75.0

chapter_identification:
  min_chapter_duration_minutes: 15
  max_chapter_duration_minutes: 120
  llm_model: "gpt-4o-mini"
  map_reduce_window_chars: 80000
  map_reduce_overlap_pct: 0.12

scoring:
  criteria:
    - name: "speaker_relevance"
      display_name: "Relevancia de los Speakers"
      max_points: 2
      description: "¿Participan líderes de partidos o gobierno?"
      examples: |
        - 2 puntos: Presidente del Gobierno, líder de la oposición, vicepresidenta
        - 1 punto: Ministros, portavoces parlamentarios
        - 0 puntos: Diputados sin relevancia mediática
    - name: "topic_relevance"
      display_name: "Actualidad de los Temas"
      max_points: 2
      description: "¿Es un tema candente en España?"
      examples: |
        - 2 puntos: Crisis nacionales, escándalos, reformas importantes
        - 1 punto: Economía, empleo, vivienda, sanidad
        - 0 puntos: Temas administrativos o técnicos
    - name: "public_interest"
      display_name: "Potencial de Interés Público"
      max_points: 1
      description: "¿Puede generar interés mediático o viral?"
      examples: |
        - 1 punto: Confrontación directa, revelaciones, potencial viral
        - 0 puntos: Debate técnico sin elementos llamativos
  min_upload_score: 2

prompts:
  chapter_identification_system: |
    Eres un experto en identificar contenido interesante en {content_type} para crear clips.
    ...
  chapter_identification_user: |
    Analiza este segmento de {content_type}...
    {domain_specific_instructions}
    ...
```

### 5.4 PipelineResult + conversión a dict para DB callback

```python
@dataclass
class Chapter:
    title: str
    description: str
    start_time: str           # "HH:MM:SS,mmm"
    end_time: str             # "HH:MM:SS,mmm"
    duration_minutes: float
    speakers: list[str]
    topics: list[str]
    timeline: list[dict]      # [{time, speaker, content}, ...]
    relevance_score: int      # 0-5
    speaker_relevance_points: int   # 0-2
    topic_relevance_points: int     # 0-2
    public_interest_points: int     # 0-1
    scoring_reasoning: str
    key_speakers: list[str]
    is_current_topic: bool

    def to_dict(self) -> dict:
        """Convertir al dict que espera el adapter de DB."""
        return {
            "title": self.title,
            "description": self.description,
            "start_time": self.start_time,
            "end_time": self.end_time,
            "duration_minutes": self.duration_minutes,
            "speakers": self.speakers,
            "topics": self.topics,
            "timeline": self.timeline,
            "relevance_score": self.relevance_score,
            "speaker_relevance_points": self.speaker_relevance_points,
            "topic_relevance_points": self.topic_relevance_points,
            "public_interest_points": self.public_interest_points,
            "scoring_reasoning": self.scoring_reasoning,
            "key_speakers": self.key_speakers,
            "is_current_topic": self.is_current_topic,
        }

@dataclass
class PipelineResult:
    video_id: str
    video_title: str
    total_chapters: int
    chapters: list[Chapter]
    errors: list[str]         # non-fatal errors (best-effort)
    srt_path: str | None      # path al SRT usado/generado

    def to_scored_chapters_dict(self) -> dict:
        """
        Convierte el resultado al formato exacto que espera
        CongressionalVideoDB.save_youtube_chapters_to_db().

        Este método es el contrato entre el motor genérico y los
        adapters de persistencia de cada dominio.
        """
        return {
            "total_videos": 1,
            "total_chapters_scored": self.total_chapters,
            "successful_scores": self.total_chapters,
            "failed_scores": 0,
            "videos": [{
                "video_id": self.video_id,
                "video_title": self.video_title,
                "total_chapters": self.total_chapters,
                "scored_chapters": [c.to_dict() for c in self.chapters],
            }],
        }
```

---

## 6. Plan de Implementación (por fases)

### Fase 1a: Crear el motor genérico (SIN tocar el Congreso)

**Objetivo:** Crear `utils/best_moments/` completo, con tests, sin modificar
**ni una sola línea** del código del Congreso. El Congreso sigue funcionando
exactamente igual con su código actual.

1. Crear `utils/best_moments/types.py` — `DomainConfig`, `ScoringCriterion`, `PipelineResult`, `Chapter`, `LLMClient` (Protocol)
2. Crear `utils/best_moments/config.py` — carga `DomainConfig` desde YAML/JSON con defaults
3. Crear `utils/best_moments/prompts.py` — prompts parametrizables con `{placeholders}`
4. Crear `utils/best_moments/scoring.py` — scoring 0-5 genérico (reimplementa `score_chapters_relevance()` sin acoplamiento al Congreso)
5. Mover `congress_videos/modules/youtube/map_reduce_chapters.py` → `utils/best_moments/map_reduce.py`
6. Crear `utils/best_moments/chunking.py` — `split_by_silence()` + `summarize_chunks()` (extraídos de `download.py`)
7. Crear `utils/best_moments/temp_files.py` — gestión de archivos temporales SRT
8. Crear `utils/best_moments/pipeline.py` — orquestador ①→⑤, con `llm_client` inyectable
9. Escribir tests unitarios con `MockLLMClient`

### Fase 1b: Refactorizar el Congreso para usar el motor

**Objetivo:** Hacer que `congress_videos/modules/youtube/download.py` delegue
en `utils/best_moments/pipeline.py`. Verificar que el DAG funciona idéntico.

1. Crear `congress_videos/config/domain_config.yaml` con la config del Congreso
2. Refactorizar `download.py` para que sus funciones sean wrappers de `pipeline()`
3. Actualizar imports en `youtube_channel_monitor_dag.py` si es necesario
4. Ejecutar `pytest tests/congress_videos/ -x -q` — deben pasar todos
5. Ejecutar `conda run -n airflow python -c "from congress_videos.youtube_channel_monitor_dag import dag"` — debe cargar sin errores

### Fase 2: Primer dominio nuevo (prueba de fuego)

1. Elegir un dominio (ej: conferencias tech)
2. Crear `configs/tech_conference.yaml`
3. Escribir tests para el nuevo dominio
4. Ejecutar el pipeline con un video real y verificar resultados

### Fase 3: Refactorización del DAG (opcional)

1. Simplificar el DAG del Congreso para usar `pipeline()` directamente
2. (Opcional) Crear un DAG factory genérico para Airflow

---

## 7. Riesgos y mitigaciones

| Riesgo | Mitigación |
|---|---|
| **Sobre-generalización** | El Congreso sigue siendo "first-class citizen" con su config validada |
| **Regresión en calidad del Congreso** | Tests de integración comparando outputs antes/después |
| **Scoring inherentemente específico del dominio** | Criterios configurables vía YAML + prompt template por dominio |
| **Complejidad del map-reduce** | Se mantiene tal cual — ya está bien desacoplado (recibe `identify_fn` inyectable) |
| **🆕 Colisión de cache LLM entre dominios** | Incluir `domain_name` en la clave de hash de `llm_cache` |
| **🆕 Archivos temporales de SRT** | El motor gestiona su propio `tempfile` lifecycle con limpieza al terminar |
| **🆕 Acoplamiento de `vad_helpers` a `constants.py`** | Convertir constantes VAD en parámetros con defaults; mover a `utils/` |

---

## 8. Cómo `scoring.py` reemplaza `score_chapters_relevance()`

Actualmente `score_chapters_relevance()` vive en `congress_videos/modules/youtube/youtube_ai.py`
y usa prompts hardcodeados de `congress_videos/config/ai_prompts.py`. En el motor genérico:

1. **`scoring.py`** contiene la lógica genérica: recibe capítulos + `DomainConfig.scoring.criteria`
2. **Construye el prompt de scoring** usando los criterios del YAML (no prompts hardcodeados)
3. **Llama al LLM** vía `llm_client.json_completion()` (inyectable)
4. **Devuelve los capítulos puntuados** con `relevance_score` y campos de scoring

El `youtube_ai.py` del Congreso se queda con las funciones que SÍ son específicas:
`generate_youtube_title()`, `generate_youtube_description()`,
`build_youtube_chapters_block()`, y `generate_youtube_metadata_for_selected_videos()`.

---

## 9. Próximos pasos

1. ✅ Plan consolidado v2 (este documento)
2. ⬜ Crear issues de GitHub para la Fase 1a
3. ⬜ Implementar `utils/best_moments/types.py` + `config.py`
4. ⬜ Implementar `utils/best_moments/prompts.py` + `scoring.py`
5. ⬜ Implementar `utils/best_moments/chunking.py` + `temp_files.py`
6. ⬜ Mover `map_reduce_chapters.py` → `utils/best_moments/map_reduce.py`
7. ⬜ Implementar `utils/best_moments/pipeline.py`
8. ⬜ Escribir tests unitarios con `MockLLMClient`
9. ⬜ Fase 1b: Refactorizar Congreso + validar que no se rompe nada
