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

Tu tarea es analizar UN SOLO CHUNK (segmento) de una sesión parlamentaria que es MAYOR de 45 minutos y dividirlo en sub-capítulos basados en TEMAS COHERENTES.

🎯 OBJETIVO PRINCIPAL: AGRUPAR POR TEMAS, NO POR HABLADORES

IMPORTANTE: Cada capítulo debe representar UN TEMA COMPLETO del debate parlamentario:
- **Tema: Vivienda** → Todo el debate sobre vivienda (preguntas + respuestas + réplicas)
- **Tema: Salario Mínimo Interprofesional** → Debate completo sobre SMI
- **Tema: Preguntas al Presidente sobre Economía** → Sesión de control sobre economía
- **Tema: Política Energética** → Debate completo sobre energía y transición

NO DIVIDAS por cambios de hablador si siguen hablando del MISMO TEMA:
❌ MAL: Dividir "Pregunta del diputado sobre vivienda" y "Respuesta del presidente sobre vivienda" en 2 capítulos
✅ BIEN: Mantener todo el intercambio sobre vivienda en 1 capítulo coherente

CONTEXTO TÉCNICO:
- Solo recibirás chunks que tengan MÁS de 45 minutos de duración
- Los chunks menores de 45 minutos se devuelven automáticamente sin análisis de IA
- Tu trabajo es dividir estos chunks largos (>45 min) en capítulos temáticos de 15-45 minutos

DURACIÓN OBJETIVO DE CADA CAPÍTULO:
- **Duración mínima: 15 minutos** (OBLIGATORIO)
- **Duración máxima: 45 minutos** (OBLIGATORIO)
- **Rango óptimo: 15-45 minutos** (ideal para YouTube)

CRITERIOS DE "TEMA INTERESANTE":
- Debates completos sobre políticas específicas (vivienda, empleo, sanidad, etc.)
- Sesiones de control al gobierno sobre un asunto concreto
- Discusión completa de una iniciativa legislativa
- Interpelaciones sobre temas de actualidad
- Confrontaciones políticas sobre asuntos relevantes

CRITERIOS DE CALIDAD:
- Cada capítulo = UN TEMA COMPLETO (con todas sus intervenciones relacionadas)
- Duración entre 15-45 minutos (ESTRICTAMENTE)
- Autocontenido (entendible sin contexto adicional)
- Dividir solo cuando cambia el TEMA principal, no el hablador

IMPORTANTE:
- Prioriza COHERENCIA TEMÁTICA sobre cambios de hablador
- Si el chunk completo trata un solo tema, devuélvelo como un solo capítulo con TODOS los campos completos
- Si hay múltiples temas distintos, divide en capítulos temáticos de 15-45 min cada uno
- Agrupa preguntas + respuestas + réplicas del MISMO tema en UN capítulo
- NUNCA devuelvas una lista vacía - SIEMPRE devuelve al menos 1 capítulo (el chunk completo si no hay divisiones claras)"""

CHAPTER_IDENTIFICATION_USER_PROMPT_TEMPLATE = """Analiza este chunk de sesión parlamentaria (>45 minutos) y divídelo en capítulos basados en TEMAS COHERENTES.

=== RESUMEN DEL CHUNK ===
{chunk_summary}

=== TRANSCRIPCIÓN COMPLETA (con timestamps) ===
{srt_content}

🎯 TAREA: Identifica los TEMAS principales del debate y crea UN capítulo por TEMA.

METODOLOGÍA DE ANÁLISIS POR TEMAS:

1. **IDENTIFICA LOS TEMAS PRINCIPALES**:
   - Lee toda la transcripción e identifica los temas políticos que se discuten
   - Ejemplos de temas: "Vivienda", "Salario Mínimo", "Política Energética", "Sanidad", etc.
   - Cada tema puede incluir múltiples habladores (pregunta + respuesta + réplica)

2. **AGRUPA INTERVENCIONES DEL MISMO TEMA**:
   - Si un diputado pregunta sobre VIVIENDA y el presidente responde sobre VIVIENDA → MISMO capítulo
   - Si hay réplica del diputado sobre VIVIENDA → Incluir en el MISMO capítulo
   - Solo crea un NUEVO capítulo cuando cambia el TEMA (ej: de Vivienda a Sanidad)

3. **RESPETA LAS DURACIONES**:
   - Cada capítulo debe durar 15-45 minutos
   - Si un tema dura más de 45 minutos, divídelo en sub-temas naturales
   - Si un tema dura menos de 15 minutos, agrúpalo con el tema siguiente relacionado

🎯 EJEMPLOS DE AGRUPACIÓN CORRECTA:

✅ CORRECTO - Agrupar por tema:
- Capítulo 1: "Debate sobre Vivienda" (30 min)
  → Incluye: pregunta diputado A + respuesta presidente + réplica diputado A + intervención diputado B

❌ INCORRECTO - Dividir por hablador:
- Capítulo 1: "Pregunta sobre vivienda" (5 min) ← Demasiado corto
- Capítulo 2: "Respuesta sobre vivienda" (8 min) ← Demasiado corto
- Capítulo 3: "Réplica sobre vivienda" (4 min) ← Demasiado corto

🔍 INDICADORES DE CAMBIO DE TEMA (no de hablador):
- "Pasamos al siguiente punto del orden del día..."
- "En relación con otro asunto..."
- Cambio claro en el contenido político (de economía a sanidad, etc.)
- Nueva pregunta sobre tema diferente

⚠️ NO SON CAMBIOS DE TEMA (ignorar para división):
- "Muchas gracias, Señor Presidente. Tiene la palabra..." ← Solo cambio de hablador
- "Le doy la palabra..." ← Solo protocolo
- Réplicas y contrarréplicas sobre el MISMO tema ← Mantener juntas

⚠️ REGLAS CRÍTICAS:
- CADA capítulo DEBE durar MÍNIMO 15 minutos
- CADA capítulo DEBE durar MÁXIMO 45 minutos
- Prioriza COHERENCIA TEMÁTICA sobre número de capítulos
- **IMPORTANTE**: Si NO encuentras temas claramente distintos para dividir, devuelve el chunk COMPLETO como UN SOLO capítulo
- SIEMPRE devuelve al menos 1 capítulo (nunca una lista vacía)

⚠️ CUANDO DEVOLVER EL CHUNK COMPLETO COMO 1 CAPÍTULO:
- Si todo el debate trata sobre el MISMO tema (ej: todo sobre vivienda)
- Si no hay puntos naturales de división entre temas
- Si dividir el chunk rompería la coherencia del debate
- En este caso, rellena TODOS los campos del capítulo:
  * title: Título descriptivo del tema principal tratado en todo el chunk
  * description: Resumen completo de lo discutido (2-3 oraciones)
  * start_time: Tiempo de inicio del chunk
  * end_time: Tiempo de fin del chunk
  * duration_minutes: Duración total del chunk
  * speakers: Lista de TODOS los habladores que participaron
  * topics: Lista de todos los temas/subtemas mencionados

FORMATO DE RESPUESTA - Devuelve un JSON con este formato:
{{
  "interesting_chapters": [
    {{
      "title": "Debate sobre [TEMA PRINCIPAL]",
      "description": "Descripción del tema discutido y las posiciones principales (2-3 oraciones)",
      "start_time": "HH:MM:SS,mmm",
      "end_time": "HH:MM:SS,mmm",
      "duration_minutes": <número entre 15-45>,
      "speakers": ["Todos los habladores que participaron en este tema"],
      "topics": ["Tema principal específico"]
    }}
  ]
}}

📌 EJEMPLOS:

Ejemplo 1 - Múltiples temas (DIVIDIR):
Si el chunk tiene "Vivienda" (25 min) + "Sanidad" (30 min) → Devuelve 2 capítulos

Ejemplo 2 - Un solo tema coherente (NO DIVIDIR):
Si el chunk tiene solo "Debate sobre vivienda" (55 min) → Devuelve 1 capítulo con todo el chunk:
{{
  "interesting_chapters": [
    {{
      "title": "Debate Completo sobre Política de Vivienda",
      "description": "Debate extenso sobre la crisis de vivienda en España, incluyendo preguntas de la oposición sobre alquiler turístico, respuesta del gobierno sobre la nueva ley de vivienda, y réplicas sobre limitación de precios de alquiler.",
      "start_time": "00:00:00,000",
      "end_time": "00:55:00,000",
      "duration_minutes": 55.0,
      "speakers": ["Portavoz PP", "Presidente Sánchez", "Portavoz Sumar", "Diputada Podemos"],
      "topics": ["Vivienda", "Alquiler turístico", "Ley de vivienda", "Precios de alquiler"]
    }}
  ]
}}

Devuelve SOLO el JSON."""
