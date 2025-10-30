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

Tu tarea es analizar UN SOLO CHUNK (segmento) de una sesión parlamentaria que es MAYOR de 45 minutos y dividirlo en sub-capítulos interesantes de duración óptima para YouTube.

CONTEXTO IMPORTANTE:
- Solo recibirás chunks que tengan MÁS de 45 minutos de duración
- Los chunks menores de 45 minutos se devuelven automáticamente sin análisis de IA (ya están en el rango óptimo)
- Tu trabajo es dividir estos chunks largos (>45 min) en capítulos más manejables

DURACIÓN OBJETIVO DE CADA CAPÍTULO:
- **Duración mínima: 15 minutos** (OBLIGATORIO - no crear capítulos más cortos)
- **Duración máxima: 45 minutos** (OBLIGATORIO - no crear capítulos más largos)
- **Rango óptimo: 15-45 minutos** (este es el "sweet spot" para YouTube)

CRITERIOS DE "INTERESANTE":
- Debates acalorados o confrontaciones entre partidos
- Anuncios de políticas importantes
- Intervenciones de figuras políticas relevantes
- Temas de actualidad o controversiales
- Momentos que generen interés público
- Cambios de tema o asunto discutido

CRITERIOS DE CALIDAD:
- Cada capítulo debe tener inicio y fin claros (no cortar a mitad de frase)
- Cada capítulo debe durar entre 15-45 minutos (ESTRICTAMENTE)
- Debe ser autocontenido (entendible sin contexto adicional)
- Dividir en límites naturales (cambios de tema, nuevos intervinientes, etc.)

IMPORTANTE:
- TODOS los capítulos que identifiques DEBEN estar entre 15-45 minutos
- Si el chunk completo es un solo tema coherente, devuélvelo como un solo capítulo (si está entre 15-45 min, aunque debería ser >45 min)
- Si hay múltiples temas, divídelos en capítulos de 15-45 minutos cada uno
- Prioriza dividir por cambios de tema naturales, no de forma arbitraria"""

CHAPTER_IDENTIFICATION_USER_PROMPT_TEMPLATE = """Analiza este chunk de sesión parlamentaria (>45 minutos) y divídelo en capítulos de duración óptima para YouTube.

=== RESUMEN DEL CHUNK ===
{chunk_summary}

=== TRANSCRIPCIÓN COMPLETA (con timestamps) ===
{srt_content}

TAREA: Divide este chunk largo en capítulos que estén entre 15-45 minutos de duración.

🎯 TRUCO PARA IDENTIFICAR CAMBIOS DE HABLADOR:
Cuando veas frases como:
- "Muchas gracias, Señor Presidente. Tiene la palabra..."
- "Tiene la palabra el señor/la señora..."
- "Le doy la palabra..."
- "Cedo la palabra..."
- Patrones similares que indican transición de hablador

Esto normalmente indica un CAMBIO DE HABLADOR. Usa estos puntos como límites naturales para dividir capítulos (siempre respetando la duración mínima de 15 minutos).

Devuelve un JSON con este formato:
{{
  "interesting_chapters": [
    {{
      "title": "Título descriptivo del capítulo",
      "description": "Breve descripción (1-2 oraciones)",
      "start_time": "HH:MM:SS,mmm",
      "end_time": "HH:MM:SS,mmm",
      "duration_minutes": <número>,
      "speakers": ["Nombre 1", "Nombre 2"],
      "topics": ["Tema principal"],
      "importance_score": <1-10>
    }}
  ]
}}

⚠️ REGLAS CRÍTICAS DE DURACIÓN:
- CADA capítulo DEBE durar MÍNIMO 15 minutos
- CADA capítulo DEBE durar MÁXIMO 45 minutos
- NO crear capítulos fuera del rango 15-45 minutos bajo ninguna circunstancia
- Si el chunk completo es un tema único coherente, devuélvelo como un solo capítulo

OTRAS REGLAS:
- Divide en límites naturales (cambios de tema, nuevos intervinientes)
- Usa los timestamps exactos del SRT
- No inventes información que no esté en la transcripción
- Prioriza calidad sobre cantidad

Devuelve SOLO el JSON."""
