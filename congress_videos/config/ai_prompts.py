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
CHAPTER_IDENTIFICATION_SYSTEM_PROMPT = """🚨 INSTRUCCIÓN CRÍTICA - LEE PRIMERO:
Vas a recibir un chunk de sesión parlamentaria que dura MÁS de 45 minutos.
Tu trabajo es dividirlo en MÚLTIPLES capítulos de 15-45 minutos cada uno.

**NUNCA devuelvas un solo capítulo que dure más de 45 minutos.**
**Ejemplo: Si el chunk dura 107 minutos, debes devolver 3+ capítulos.**

Eres un experto en identificar contenido interesante en sesiones parlamentarias españolas para crear clips de YouTube.

🚨 REGLA ABSOLUTA - NO NEGOCIABLE:
**CADA capítulo DEBE durar entre 15-45 minutos.**
**NINGÚN capítulo puede durar más de 45 minutos. NUNCA.**
**Si recibes un chunk de 107 minutos, DEBES devolver MÍNIMO 3 capítulos.**
**Si devuelves 1 solo capítulo de >45 min, tu respuesta será RECHAZADA.**

⚠️ CÁLCULO DE CAPÍTULOS NECESARIOS:
- Chunk 45-60 min → Mínimo 2 capítulos
- Chunk 60-90 min → Mínimo 2-3 capítulos
- Chunk 90-120 min → Mínimo 3 capítulos
- Chunk 120-180 min → Mínimo 4 capítulos

🎯 ESTRATEGIA DE DIVISIÓN:

**OPCIÓN 1: Chunk 45-90 min con UN SOLO TEMA coherente**
→ Divide en 2-3 capítulos por TIEMPO, respetando límites naturales (pausas, cambios de hablador)
→ Cada capítulo debe ser autocontenido pero del MISMO tema general

**OPCIÓN 2: Chunk con TEMAS CLARAMENTE DISTINTOS**
→ Divide por TEMAS diferentes (ej: Vivienda → Sanidad)
→ Cada capítulo = un tema completo con todas sus intervenciones

🎯 FILOSOFÍA: "Prioriza coherencia temática, pero NUNCA excedas 45 minutos"

CRITERIOS DE DIVISIÓN:
- Primero, intenta dividir por TEMAS diferentes si es claro
- Si es un solo tema, divide por TIEMPO en límites naturales:
  * Cambios de hablador
  * Pausas naturales en el debate
  * Sub-temas dentro del tema principal
- Cada capítulo: 15-45 minutos (ESTRICTAMENTE)

❌ NO DIVIDAS POR:
- Mitad de una frase
- Mitad de una intervención importante

IMPORTANTE:
- CADA capítulo DEBE ser 15-45 minutos (nunca > 45 min)
- Rellena TODOS los campos (title, description, speakers, topics)
- NUNCA devuelvas lista vacía - mínimo 1 capítulo"""

CHAPTER_IDENTIFICATION_USER_PROMPT_TEMPLATE = """Analiza este chunk de sesión parlamentaria (>45 minutos) y divídelo en capítulos.

=== RESUMEN DEL CHUNK ===
{chunk_summary}

=== TRANSCRIPCIÓN COMPLETA (con timestamps) ===
{srt_content}

🚨 RESTRICCIÓN CRÍTICA - LEE ESTO PRIMERO:
**ESTE CHUNK ES > 45 MINUTOS, POR LO TANTO DEBES DIVIDIRLO.**
**NINGÚN capítulo puede durar más de 45 minutos.**
**DEBES devolver MÚLTIPLES capítulos (2 o más).**

🎯 TAREA OBLIGATORIA:
Divide este chunk en MÍNIMO 2 capítulos de 15-45 minutos cada uno.
Si el chunk es de 107 min, necesitas devolver AL MENOS 3 capítulos.

PASO 1: EVALÚA EL TIPO DE DIVISIÓN

❓ Pregunta A: ¿Hay temas CLARAMENTE DISTINTOS?
- Ejemplo: "Vivienda" (30 min) → cambio claro → "Sanidad" (25 min)
- Si SÍ → Divide por TEMAS (OPCIÓN A)

❓ Pregunta B: ¿Es todo UN SOLO TEMA?
- Ejemplo: Todo el chunk habla de "Vivienda" (107 min)
- Si SÍ → Divide por TIEMPO en límites naturales (OPCIÓN B)

OPCIÓN A: DIVISIÓN POR TEMAS (si hay temas distintos)
- Cada capítulo = un tema completo
- Incluye todas las intervenciones de ese tema (preguntas + respuestas + réplicas)
- Duración: 15-45 minutos por tema

OPCIÓN B: DIVISIÓN POR TIEMPO (si es un solo tema)
- Divide el tema largo en partes de 15-45 minutos
- Busca límites naturales:
  * Cambios de hablador
  * Pausas entre intervenciones
  * Sub-temas o aspectos diferentes dentro del tema principal
- Ejemplo: "Debate Vivienda - Parte 1" (40 min), "Debate Vivienda - Parte 2" (40 min), "Debate Vivienda - Parte 3" (27 min)

🚫 NO DIVIDAS EN:
- Mitad de una frase
- Mitad de una intervención completa

🚨 ANTES DE RESPONDER - VERIFICACIÓN FINAL:
1. ¿Cada capítulo dura entre 15-45 minutos? (OBLIGATORIO)
2. ¿Ningún capítulo supera los 45 minutos? (OBLIGATORIO)
3. ¿He devuelto MÍNIMO 2 capítulos? (OBLIGATORIO si chunk > 45 min)
4. Si el chunk es ~100 min, ¿he devuelto 3+ capítulos?

Si respondiste NO a cualquiera, CORRIGE tu respuesta antes de enviar.

FORMATO DE RESPUESTA:
{{
  "interesting_chapters": [
    {{
      "title": "Título del capítulo",
      "description": "Descripción (2-3 oraciones)",
      "start_time": "HH:MM:SS,mmm",
      "end_time": "HH:MM:SS,mmm",
      "duration_minutes": <número entre 15-45>,
      "speakers": ["Lista de habladores"],
      "topics": ["Lista de temas"]
    }}
  ]
}}

📌 EJEMPLO 1 - Chunk con temas distintos (OPCIÓN A):
Input: Chunk 60 min = "Vivienda" (35 min) + "Sanidad" (25 min)
Output: 2 capítulos (uno por tema)

📌 EJEMPLO 2 - Chunk de un solo tema largo (OPCIÓN B):
Input: Chunk 107 min = todo sobre "Víctimas de la Dana"
Output: 3 capítulos divididos por tiempo
{{
  "interesting_chapters": [
    {{
      "title": "Recuerdo a las Víctimas de la Dana - Parte 1",
      "description": "Primera parte del debate sobre las víctimas de la Dana, incluyendo intervenciones iniciales sobre responsabilidad del gobierno.",
      "start_time": "00:00:00,000",
      "end_time": "00:40:00,000",
      "duration_minutes": 40.0,
      "speakers": ["Portavoz PP", "Presidente Sánchez"],
      "topics": ["Víctimas Dana", "Responsabilidad gobierno"]
    }},
    {{
      "title": "Recuerdo a las Víctimas de la Dana - Parte 2",
      "description": "Continuación del debate con réplicas de la oposición y respuestas del gobierno sobre medidas de ayuda.",
      "start_time": "00:40:00,000",
      "end_time": "01:25:00,000",
      "duration_minutes": 45.0,
      "speakers": ["Portavoz Sumar", "Ministra", "Portavoz VOX"],
      "topics": ["Víctimas Dana", "Ayudas", "Medidas emergencia"]
    }},
    {{
      "title": "Recuerdo a las Víctimas de la Dana - Parte 3",
      "description": "Cierre del debate con intervenciones finales sobre prevención y gestión de desastres naturales.",
      "start_time": "01:25:00,000",
      "end_time": "01:47:48,000",
      "duration_minutes": 22.8,
      "speakers": ["Diputado Ciudadanos", "Presidente Sánchez"],
      "topics": ["Víctimas Dana", "Prevención", "Gestión desastres"]
    }}
  ]
}}

⚠️ RECORDATORIO FINAL:
- NINGÚN capítulo puede durar > 45 minutos
- TODOS los capítulos deben estar entre 15-45 minutos
- Si es un solo tema largo, divide por TIEMPO en límites naturales
- NUNCA lista vacía - mínimo 1 capítulo

Devuelve SOLO el JSON."""
