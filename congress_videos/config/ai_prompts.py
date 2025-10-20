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

# Chapter Analysis - Topic Change Detection
CHAPTER_ANALYSIS_SYSTEM_PROMPT_TEMPLATE = """Estás analizando la transcripción de una sesión parlamentaria para identificar cambios de tema.

Tu tarea es simple:
1. Lee la transcripción e identifica cuándo cambia el tema/asunto
2. Marca límites naturales donde termina un tema y comienza otro
3. Crea capítulos de {min_duration}-{max_duration} minutos en estos cambios de tema
4. No cortes en medio de una frase o intervención

🔥 FLEXIBILIDAD TOTAL EN NÚMERO DE CAPÍTULOS:
- Puedes crear TANTOS capítulos como sean necesarios según los cambios de tema
- NO hay límite máximo de capítulos - crea 2, 5, 10, 20, o los que necesites
- Si hay muchos temas diferentes, crea muchos capítulos
- Si hay pocos temas, crea pocos capítulos
- Puedes crear capítulos que cubran TODO el vídeo completo si el tema es único
- Puedes crear UN SOLO capítulo que abarque toda la transcripción si no hay cambios de tema significativos

⚠️ IMPORTANTE - DURACIÓN MÍNIMA:
- CADA capítulo DEBE durar MÍNIMO {min_duration} minutos
- NO crear capítulos más cortos de {min_duration} minutos bajo ninguna circunstancia
- Es preferible tener menos capítulos largos que muchos capítulos cortos

FORMATO DE SALIDA:
Devuelve SOLO un objeto JSON válido (sin markdown, sin bloques de código):
{{
  "chapters": [
    {{
      "chapter_number": 1,
      "title": "Breve descripción del tema",
      "start_time": "HH:MM:SS",
      "end_time": "HH:MM:SS",
      "topics": ["tema principal discutido"]
    }}
  ]
}}"""

CHAPTER_ANALYSIS_USER_PROMPT_TEMPLATE = """Identifica los cambios de tema en esta transcripción de sesión parlamentaria.

=== ORDEN DEL DÍA (para contexto) ===
{agenda_content}

=== TRANSCRIPCIÓN (con marcas de tiempo) ===
{srt_content}

TAREA: Identifica dónde cambian los temas y crea capítulos en límites naturales.

🔥 FLEXIBILIDAD TOTAL:
- Crea TANTOS capítulos como necesites - no hay límite máximo
- Si identificas 1 tema → crea 1 capítulo que cubra TODO el vídeo
- Si identificas 3 temas → crea 3 capítulos
- Si identificas 10 temas → crea 10 capítulos
- Puedes crear un capítulo único para toda la transcripción si es apropiado
- La cantidad de capítulos depende ÚNICAMENTE de los cambios de tema que identifiques

⚠️ REQUISITOS CRÍTICOS DE DURACIÓN:
- Cada capítulo DEBE durar MÍNIMO {min_duration_minutes} minutos (esto es OBLIGATORIO)
- Duración máxima: {max_duration_minutes} minutos
- Elimina el silencio del comienzo de la transcripción, no la emplees en nigun capítulo.
- NO crear capítulos de menos de {min_duration_minutes} minutos

Otros requisitos:
- No cortes en medio de una discusión
- Dale a cada capítulo un título descriptivo simple
- IMPORTANTE: Puedes y DEBES cubrir toda la duración del vídeo con tus capítulos

Devuelve SOLO el objeto JSON."""

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

Tu tarea es analizar UN SOLO CHUNK (segmento) de una sesión parlamentaria y encontrar momentos/capítulos interesantes que puedan extraerse como clips independientes para YouTube.

NOTA IMPORTANTE: Solo recibirás chunks que tengan 15 minutos o más de duración. Los chunks menores de 15 minutos se devuelven automáticamente sin análisis de IA.

CRITERIOS DE "INTERESANTE":
- Debates acalorados o confrontaciones entre partidos
- Anuncios de políticas importantes
- Intervenciones de figuras políticas relevantes
- Temas de actualidad o controversiales
- Momentos que generen interés público

CRITERIOS DE CALIDAD:
- Cada capítulo debe tener inicio y fin claros (no cortar a mitad de frase)
- Duración mínima: 5 minutos (ya que el chunk completo tiene mínimo 15 minutos)
- Duración máxima: hasta el tamaño del chunk completo
- Debe ser autocontenido (entendible sin contexto adicional)

IMPORTANTE:
- Puedes encontrar 0, 1, o varios capítulos interesantes en el chunk
- Si no hay nada interesante, devuelve una lista vacía
- Si hay múltiples temas interesantes, identifícalos todos
- Si TODO el chunk es interesante y coherente, puedes devolver el chunk completo como un solo capítulo"""

CHAPTER_IDENTIFICATION_USER_PROMPT_TEMPLATE = """Analiza este chunk de sesión parlamentaria y encuentra capítulos interesantes.

=== RESUMEN DEL CHUNK ===
{chunk_summary}

=== TRANSCRIPCIÓN COMPLETA (con timestamps) ===
{srt_content}

TAREA: Identifica capítulos interesantes dentro de este chunk que valga la pena extraer como clips de YouTube.

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

REGLAS:
- Si NO encuentras nada interesante, devuelve: {{"interesting_chapters": []}}
- Usa los timestamps exactos del SRT
- No inventes información que no esté en la transcripción
- Prioriza calidad sobre cantidad

Devuelve SOLO el JSON."""
