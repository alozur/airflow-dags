"""
AI Prompts for YouTube content generation.

This module contains all AI prompts used for generating YouTube metadata
and evaluating video content for the Congreso YouTube project.
"""

# YouTube Title Generation
YOUTUBE_TITLE_SYSTEM_PROMPT = (
    "Eres un experto en crear títulos atractivos para contenido político español en YouTube."
)

YOUTUBE_TITLE_USER_PROMPT_TEMPLATE = """
Genera un título optimizado para YouTube de un vídeo del Congreso de España. El título debe ser:
- Atractivo y claro para audiencia general
- Máximo {max_length} caracteres
- Incluir palabras clave relevantes
- Evitar jerga política compleja

Contenido del debate: {main_topic_content}
{speaker_context}

Genera solo el título, sin explicaciones adicionales y sin comillas.
"""

# YouTube Description Generation
YOUTUBE_DESCRIPTION_SYSTEM_PROMPT = (
    "Eres un periodista político español experto en comunicar de forma clara y atractiva. "
    "Usas un lenguaje natural, cercano pero profesional, y estructuras bien el contenido con saltos de línea."
)

YOUTUBE_DESCRIPTION_USER_PROMPT_TEMPLATE = """
Crea una descripción para YouTube de un debate del Congreso español. La descripción debe:

- Ser natural y conversacional (no robótica)
- Usar saltos de línea para separar secciones claramente
- Explicar el contexto político de forma accesible
- Incluir emojis relevantes para hacer más atractivo el contenido
- Terminar con hashtags españoles relevantes

CONTENIDO DEL DEBATE:
{main_topic_content}

INFORMACIÓN ADICIONAL:
- Sesión número: {session_number}
- {speaker_context}
- {duration_info}

ESTRUCTURA REQUERIDA:
1. Párrafo introductorio explicando el tema (con emojis)
2. Salto de línea doble
3. Contexto político o relevancia del debate
4. Salto de línea doble
5. Información sobre los participantes
6. Salto de línea doble
7. Hashtags relevantes (mínimo 5)

Escribe de forma natural, como si fueras un periodista explicando el debate a ciudadanos interesados en política.
"""

# Video Interest Evaluation - Main Topic
VIDEO_INTEREST_MAIN_TOPIC_SYSTEM_PROMPT = (
    "Eres un experto en política española y contenido viral para YouTube. "
    "Evalúas objetivamente el interés público de debates parlamentarios."
)

VIDEO_INTEREST_MAIN_TOPIC_USER_PROMPT_TEMPLATE = """Evalúa el interés de este vídeo del Congreso de los Diputados para subirlo a YouTube.

TEMA: {topic_content}

PARTICIPANTES:
{speakers_text}

DURACIÓN: {duration_minutes} minutos

Criterios de evaluación:
1. Relevancia política y social (0-3 puntos)
2. Notoriedad de los participantes (0-3 puntos)
3. Potencial de debate público (0-2 puntos)
4. Interés mediático (0-2 puntos)

Responde SOLO con un JSON en este formato:
{{"score": <número del 1-10>, "reasoning": "<explicación breve en español>"}}"""

# Video Interest Evaluation - Intervention
VIDEO_INTEREST_INTERVENTION_SYSTEM_PROMPT = (
    "Eres un experto en política española y contenido viral para YouTube. "
    "Evalúas objetivamente el interés público de intervenciones parlamentarias."
)

VIDEO_INTEREST_INTERVENTION_USER_PROMPT_TEMPLATE = """Evalúa el interés de esta intervención parlamentaria individual para subirla a YouTube.

CONTEXTO DEL TEMA: {main_topic_context}

INTERVINIENTE: {speaker_name}
CARGO: {speaker_role}
DURACIÓN: {duration_minutes} minutos

Criterios de evaluación:
1. Relevancia del tema (0-3 puntos)
2. Notoriedad del interviniente (0-3 puntos)
3. Duración apropiada para YouTube (0-2 puntos)
4. Potencial de interés público (0-2 puntos)

Responde SOLO con un JSON en este formato:
{{"score": <número del 1-10>, "reasoning": "<explicación breve en español>"}}"""

# Thumbnail Text Generation
THUMBNAIL_TEXT_SYSTEM_PROMPT = (
    "Eres un experto en crear texto impactante para miniaturas de YouTube. "
    "Tu objetivo es crear frases MUY CORTAS (3-6 palabras) que capten atención y generen clics."
)

THUMBNAIL_TEXT_USER_PROMPT_TEMPLATE = """Crea una frase ULTRA CORTA para miniatura de YouTube de un vídeo del Congreso.

Título: {video_title}
Contexto: {video_description}

REQUISITOS CRÍTICOS:
- MÁXIMO 3-6 palabras
- Máximo {max_length} caracteres en total (incluyendo espacios)
- Lenguaje directo e impactante
- Generar curiosidad o urgencia
- Sin signos de interrogación ni comillas
- Palabras clave que llamen la atención
- NO CORTAR PALABRAS: el texto debe caber en {max_length} caracteres sin cortar ninguna palabra

Ejemplos de buen estilo:
- "REFORMA PENSIONES: ¡DEBATE EXPLOSIVO!"
- "CRISIS ENERGÉTICA REVELADA"
- "GOBIERNO: POLÍTICAS SECRETAS"

IMPORTANTE: Si la frase supera {max_length} caracteres, acórtala eliminando palabras completas, NUNCA cortando palabras a la mitad.

Devuelve SOLO la frase, sin explicaciones."""

# Chunk Summarization - For silence-based chunks before chapter analysis
CHUNK_SUMMARY_SYSTEM_PROMPT = """Eres un experto en analizar transcripciones de sesiones parlamentarias españolas.

Tu tarea es extraer información estructurada de un segmento de transcripción:
- SPEAKERS: Identifica quién habló, cuándo empezó y cuándo terminó cada intervención
- TOPICS: Identifica los temas principales discutidos
- TIMELINE: Crea una línea temporal con las intervenciones clave
- SUMMARY: Resumen general del segmento

IMPORTANTE: Devuelve SIEMPRE un JSON válido con la estructura exacta especificada."""

CHUNK_SUMMARY_USER_PROMPT_TEMPLATE = """Analiza este segmento de sesión parlamentaria y extrae la información estructurada.

INFORMACIÓN DEL SEGMENTO:
- Chunk: {chunk_number}
- Tiempo de inicio: {start_time}
- Tiempo de fin: {end_time}
- Duración: {duration_minutes} minutos

TRANSCRIPCIÓN:
{chunk_content}

TAREA: Extrae la siguiente información en formato JSON:

{{
  "speakers": [
    {{
      "name": "Nombre del interviniente",
      "role": "Cargo o grupo parlamentario",
      "start_time": "HH:MM:SS",
      "end_time": "HH:MM:SS"
    }}
  ],
  "topics": [
    "Tema 1 discutido",
    "Tema 2 discutido"
  ],
  "timeline": [
    {{
      "time": "HH:MM:SS",
      "speaker": "Nombre",
      "content": "Resumen breve de lo que dijo (1 frase)"
    }}
  ],
  "summary": "Resumen general del chunk en 2-3 oraciones"
}}

INSTRUCCIONES:
- Identifica TODOS los intervinientes que hablaron en este segmento
- Para cada interviniente, indica cuándo empezó y terminó su intervención (usa timestamps del SRT)
- Lista los temas principales (no procedimientos formales)
- Crea una timeline con las intervenciones más relevantes
- El resumen debe explicar qué se discutió, no cómo se organizó la sesión

Devuelve SOLO el JSON, sin markdown ni explicaciones."""

# Chapter Identification - Identify interesting sub-chapters within each chunk
CHAPTER_IDENTIFICATION_SYSTEM_PROMPT = """Eres un experto en identificar contenido interesante en sesiones parlamentarias españolas para crear clips de YouTube.

Tu tarea es analizar UN CHUNK de sesión parlamentaria que dura MÁS de 45 minutos y decidir si dividirlo o mantenerlo completo.

🎯 FILOSOFÍA: "Prioriza COHERENCIA TEMÁTICA sobre duración"

⚠️ REGLAS DE DIVISIÓN:

**Opción 1: Mantener como 1 capítulo completo**
- Si el chunk trata UN SOLO TEMA coherente
- Duración ≤ 120 minutos (2 horas)
- Puedes hacerlo aunque haya sub-temas o múltiples habladores
- Ejemplo: Debate de 90 min sobre "Vivienda" → PUEDE ser 1 capítulo de 90 min

**Opción 2: Dividir en múltiples capítulos**
- Si identificas SUB-TEMAS naturales dentro del tema principal
- Si hay TEMAS CLARAMENTE DISTINTOS (ej: Vivienda → Sanidad)
- Si el chunk > 120 minutos (entonces DEBES dividir)
- Ejemplo: Debate de 90 min sobre "Vivienda" → TAMBIÉN puede ser 2 capítulos si identificas sub-temas claros

**Flexibilidad:**
- Tú decides cuándo dividir según el contenido
- No hay reglas estrictas, usa tu criterio
- Prioriza coherencia sobre número de capítulos

🔍 CRITERIOS PARA DIVIDIR POR TIEMPO (solo si >120 min y un solo tema):
- Busca PAUSAS NATURALES de 4-5+ segundos en la transcripción
- Cambios de hablador en pausas largas
- NUNCA dividas en medio de una intervención
- NUNCA dividas en medio de una frase
- El número de capítulos es VARIABLE según el contenido

⚠️ RESTRICCIONES:
- Mínimo 15 minutos por capítulo
- Máximo 120 minutos por capítulo (solo si es tema único coherente)
- Si divides, hazlo en pausas naturales (4-5+ segundos de silencio)

IMPORTANTE:
- Prioriza mantener temas completos juntos
- Solo divide por tiempo si supera 2 horas
- Rellena TODOS los campos (title, description, speakers, topics)
- NUNCA devuelvas lista vacía - mínimo 1 capítulo"""

CHAPTER_IDENTIFICATION_USER_PROMPT_TEMPLATE = """Analiza este chunk de sesión parlamentaria (>45 minutos).

=== RESUMEN DEL CHUNK ===
{chunk_summary}

=== TRANSCRIPCIÓN COMPLETA (con timestamps) ===
{srt_content}

🎯 TAREA: Decide si mantener el chunk completo o dividirlo.

PASO 1: EVALÚA EL CONTENIDO TEMÁTICO

❓ Pregunta Principal: ¿Cómo está organizado el contenido?

**OPCIÓN A: Mantener como 1 capítulo**
- Si el chunk tiene UN TEMA principal coherente
- Duración ≤ 120 minutos
- Aunque tenga sub-temas o múltiples habladores
- Ejemplo: 90 min sobre "Víctimas de la Dana" → 1 capítulo completo (si es coherente)

**OPCIÓN B: Dividir en 2+ capítulos**
- Si identificas SUB-TEMAS naturales dentro del tema
- Si hay TEMAS CLARAMENTE DISTINTOS (ej: Vivienda → Sanidad)
- Si el chunk > 120 minutos (OBLIGATORIO dividir)
- Ejemplo: 90 min sobre "Política Económica" → 2 capítulos si identificas "Inflación" + "Empleo" como sub-temas

**Usa tu criterio:**
- No hay fórmula fija
- Prioriza que cada capítulo sea coherente y autocontenido
- Puedes dividir un chunk de 80 min si ves sub-temas claros
- Puedes mantener un chunk de 110 min si es un solo tema coherente

PASO 2: SI NECESITAS DIVIDIR POR TIEMPO (solo si un tema > 120 min)

🔍 Busca PAUSAS NATURALES en la transcripción:
- Silencios de 4-5+ segundos entre intervenciones
- Cambios de hablador en pausas largas
- Finales de intervenciones completas

🚫 NUNCA DIVIDAS EN:
- Medio de una intervención
- Medio de una frase
- Sin pausa natural de al menos 4-5 segundos

📐 Número de capítulos:
- Es VARIABLE según el contenido
- Ejemplo: 165 min → 2 capítulos (85 min + 80 min)
- Ejemplo: 180 min → 2 capítulos (90 min + 90 min)
- NO hay fórmulas fijas, depende de las pausas naturales

FORMATO DE RESPUESTA:
{{
  "interesting_chapters": [
    {{
      "title": "Título del capítulo",
      "description": "Descripción (2-3 oraciones)",
      "start_time": "HH:MM:SS,mmm",
      "end_time": "HH:MM:SS,mmm",
      "duration_minutes": <número>,
      "speakers": ["Lista de habladores"],
      "topics": ["Lista de temas"]
    }}
  ]
}}

📌 EJEMPLO 1 - Chunk de 90 min, UN SOLO TEMA:
Input: Chunk 90 min sobre "Víctimas de la Dana"
Output: 1 capítulo completo (≤ 120 min, OK)
{{
  "interesting_chapters": [
    {{
      "title": "Recuerdo a las Víctimas de la Dana y Responsabilidad del Gobierno",
      "description": "Debate completo sobre las víctimas de la Dana, incluyendo todas las intervenciones sobre responsabilidad, ayudas y prevención.",
      "start_time": "00:00:00,000",
      "end_time": "01:30:00,000",
      "duration_minutes": 90.0,
      "speakers": ["Portavoz PP", "Presidente Sánchez", "Portavoz Sumar", "Ministra"],
      "topics": ["Víctimas Dana", "Responsabilidad gobierno", "Ayudas", "Prevención"]
    }}
  ]
}}

📌 EJEMPLO 2 - Chunk de 165 min, UN SOLO TEMA (> 120 min):
Input: Chunk 165 min sobre "Política Energética"
Output: 2 capítulos divididos en pausas naturales
{{
  "interesting_chapters": [
    {{
      "title": "Debate sobre Política Energética - Parte 1",
      "description": "Primera parte del extenso debate energético, incluyendo precios de la luz y energías renovables.",
      "start_time": "00:00:00,000",
      "end_time": "01:25:00,000",
      "duration_minutes": 85.0,
      "speakers": ["..."],
      "topics": ["Energía", "Precios luz", "Renovables"]
    }},
    {{
      "title": "Debate sobre Política Energética - Parte 2",
      "description": "Continuación del debate con dependencia energética y transición ecológica.",
      "start_time": "01:25:00,000",
      "end_time": "02:45:00,000",
      "duration_minutes": 80.0,
      "speakers": ["..."],
      "topics": ["Energía", "Dependencia energética", "Transición"]
    }}
  ]
}}

📌 EJEMPLO 3 - Chunk con temas distintos:
Input: Chunk 80 min = "Vivienda" (50 min) + "Sanidad" (30 min)
Output: 2 capítulos (uno por tema)

⚠️ RECORDATORIO FINAL:
- Mínimo 15 minutos por capítulo
- Máximo 120 minutos por capítulo (si es tema único)
- Solo divide por tiempo si el chunk > 120 minutos
- Divide en pausas naturales (4-5+ segundos)
- NUNCA lista vacía - mínimo 1 capítulo

Devuelve SOLO el JSON."""

# Chapter Relevance Scoring - Score chapters from 1-5 based on political relevance
CHAPTER_RELEVANCE_SCORING_SYSTEM_PROMPT = """Eres un experto en política española que evalúa la relevancia de debates parlamentarios para contenido de YouTube.

Tu tarea es asignar un score de 1 a 5 basándote en múltiples criterios de relevancia política y mediática.

CRITERIOS DE EVALUACIÓN:

1. **Relevancia de los Speakers (0-2 puntos)**
   - 2 puntos: Líderes principales de partidos o gobierno
     * Presidente del Gobierno (Pedro Sánchez)
     * Líder de la oposición (Alberto Núñez Feijóo)
     * Vicepresidenta (Yolanda Díaz)
     * Otros líderes de partidos (Santiago Abascal, etc.)
   - 1 punto: Ministros, portavoces parlamentarios, diputados prominentes
   - 0 puntos: Diputados sin gran relevancia mediática

2. **Actualidad y Relevancia de los Temas (0-2 puntos)**
   - 2 puntos: Temas MUY candentes o de GRAN actualidad
     * Crisis nacionales (Dana, desastres naturales)
     * Escándalos políticos recientes
     * Reformas legislativas importantes
     * Temas que dominan los medios actualmente
   - 1 punto: Temas de interés medio (economía, empleo, vivienda, sanidad)
   - 0 puntos: Temas administrativos, técnicos o de bajo interés público

3. **Potencial de Interés Público (0-1 punto)**
   - 1 punto: El debate tiene elementos que pueden generar interés mediático
     * Confrontación directa entre líderes
     * Revelaciones importantes
     * Temas que afectan directamente a la ciudadanía
     * Potencial viral o de gran repercusión
   - 0 puntos: Debate técnico sin elementos llamativos

ESCALA FINAL (suma de puntos):
- 5 puntos: MÁXIMA relevancia → DEBE subirse a YouTube
- 4 puntos: ALTA relevancia → Muy recomendado subir
- 3 puntos: Relevancia MEDIA → Considerar subir
- 2 puntos: BAJA relevancia → Probablemente no subir
- 1 punto: MUY BAJA relevancia → No subir

IMPORTANTE: Sé objetivo y evalúa la relevancia real para el público español general, no solo para expertos en política."""

CHAPTER_RELEVANCE_SCORING_USER_PROMPT_TEMPLATE = """Evalúa la relevancia de este capítulo de sesión parlamentaria para contenido de YouTube.

=== INFORMACIÓN DEL CAPÍTULO ===
Título: {chapter_title}
Descripción: {chapter_description}
Duración: {duration_minutes} minutos

=== SPEAKERS QUE PARTICIPAN ===
{speakers_list}

=== TEMAS TRATADOS ===
{topics_list}

TAREA: Evalúa este capítulo usando los criterios establecidos y asigna un score de 1 a 5.

PASO 1: Evalúa cada criterio individualmente
1. Relevancia de speakers (0-2 puntos): ¿Quiénes participan? ¿Son figuras políticas relevantes?
2. Actualidad de temas (0-2 puntos): ¿Es un tema candente en España ahora mismo?
3. Potencial de interés público (0-1 punto): ¿Puede generar interés mediático o viral?

PASO 2: Suma los puntos y justifica tu evaluación

FORMATO DE RESPUESTA (JSON):
{{
  "score": <número del 1-5>,
  "speaker_relevance_points": <0-2>,
  "topic_relevance_points": <0-2>,
  "public_interest_points": <0-1>,
  "reasoning": "<explicación breve en español de por qué asignaste este score, mencionando speakers clave y temas>",
  "key_speakers": ["<lista de speakers más relevantes>"],
  "is_current_topic": <true/false - si el tema es de actualidad NOW>
}}

IMPORTANTE:
- Devuelve SOLO el JSON, sin markdown ni explicaciones adicionales
- Sé crítico y objetivo: no todos los debates merecen score alto
- El score debe reflejar el interés REAL para audiencia general de YouTube
- Considera el contexto político español actual (fecha: octubre 2025)

Devuelve SOLO el JSON."""
