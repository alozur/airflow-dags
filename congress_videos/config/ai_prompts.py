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
