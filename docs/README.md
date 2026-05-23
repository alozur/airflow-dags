# airflow-dags

Repositorio de DAGs de Apache Airflow. El proyecto principal es **congress_videos**: un pipeline automático que monitoriza el canal oficial de YouTube del Congreso de los Diputados de España, descarga y transcribe sesiones plenarias, analiza el contenido con IA y sube los mejores fragmentos a YouTube como vídeos independientes.

---

## Propósito

El objetivo es automatizar el procesamiento completo de sesiones parlamentarias:

1. Detectar sesiones plenarias nuevas en el canal de YouTube del Congreso.
2. Descargar el vídeo y obtener subtítulos o transcribir el audio con Whisper.
3. Dividir la sesión en fragmentos temáticos usando IA (GPT-4o-mini).
4. Puntuar cada fragmento por relevancia política (escala 0-5).
5. Subir los fragmentos más relevantes a YouTube con título, descripción y miniatura generados por IA.

---

## Estructura de carpetas

```
airflow-dags/
├── congress_videos/               # Proyecto principal de vídeos parlamentarios
│   ├── config/
│   │   ├── ai_prompts.py          # Todos los prompts de IA (títulos, descripciones, scoring…)
│   │   ├── constants.py           # Canal de YouTube, IDs, parámetros globales
│   │   └── paths.py               # Rutas centralizadas del sistema de ficheros
│   ├── modules/
│   │   ├── database.py            # Clase CongressionalVideoDB: operaciones CRUD
│   │   ├── postgres_operators.py  # Operador Airflow personalizado para PostgreSQL
│   │   ├── speaker_helpers.py     # Formateo de información de intervinientes
│   │   ├── thumbnail_generator.py # Generación de miniaturas con Pillow + IA
│   │   ├── video_splitter.py      # Extracción de capítulos con ffmpeg
│   │   ├── web_scraping.py        # Construcción de URLs de sesiones del Congreso
│   │   └── youtube/
│   │       ├── download.py        # Descarga de vídeo/audio desde YouTube
│   │       ├── youtube_ai.py      # Generación de metadatos y scoring con OpenAI
│   │       ├── youtube_channel.py # Monitorización del canal (YouTube Data API)
│   │       └── youtube_upload.py  # Preparación de configuración para el uploader
│   ├── scripts/
│   │   └── generate_youtube_token.py  # Genera token OAuth para YouTube
│   ├── sql/
│   │   ├── congressional_videos_schema.sql  # Schema original (sesiones y tópicos)
│   │   ├── youtube_chapters_schema.sql      # Schema de capítulos y vídeos fuente
│   │   ├── production_schema.sql            # Schema de producción
│   │   ├── grant_permissions.sql            # Permisos para entorno de desarrollo
│   │   └── grant_permissions_production.sql # Permisos para producción
│   ├── youtube_channel_monitor_dag.py  # DAG principal: monitorización y análisis
│   ├── youtube_upload_dag.py           # DAG de subida de capítulos a YouTube
│   └── requirements.txt
│
├── utils/                         # Utilidades compartidas entre todos los proyectos
│   ├── airflow_helpers.py         # xcom_task wrapper + ensure_project_data_directory
│   ├── ai_chapter_analyzer.py     # Análisis de capítulos con IA (chunking por silencios)
│   ├── ai_helpers.py              # Wrappers genéricos para OpenAI API
│   ├── env_loader.py              # Carga de .env en local; no-op en Docker
│   ├── git_sync_dag.py            # DAG de sincronización manual de git
│   ├── postgres_helpers.py        # PostgresConnection: gestión de conexiones psycopg2
│   ├── whisper_helpers.py         # Transcripción con Whisper (local o Docker API)
│   ├── youtube_downloader.py      # Descarga de vídeo/audio con yt-dlp y pytubefix
│   └── youtube_uploader_dag.py    # DAG genérico reutilizable de subida a YouTube
│
├── examples/
│   ├── hello_world_dag.py         # DAG mínimo de ejemplo
│   └── hello_weekly_dag.py        # DAG semanal de ejemplo
│
├── docs/
│   ├── README.md                  # Este fichero
│   ├── ARCHITECTURE.md            # Arquitectura y flujo de datos
│   ├── DAGS.md                    # Documentación detallada de cada DAG
│   └── architecture/              # Documentación legada (tech-stack, coding-standards…)
│
├── Dockerfile                     # Imagen custom de Airflow (ffmpeg + nodejs + git)
├── docker-compose.yml             # Despliegue de desarrollo (puerto 8081)
├── docker-compose.prod.yml        # Despliegue de producción (puerto 8082)
├── docker-compose-whisper.yml     # Servicio Whisper ASR independiente
└── .github/workflows/
    └── github_sync_main_development.yml  # Sincroniza main → dev tras PR merged
```

---

## DAGs disponibles

### 1. `congress_youtube_channel_monitor`
**Fichero:** `congress_videos/youtube_channel_monitor_dag.py`

Monitoriza el canal YouTube del Congreso, obtiene la sesión plenaria del día, la transcribe, la analiza con IA para identificar capítulos relevantes y los guarda en PostgreSQL con puntuación de 0 a 5.

**Schedule:** `0 22 * * *` (22:00 cada día)
**Parámetros:**
- `target_date`: Fecha a procesar (por defecto ayer, formato `YYYY-MM-DD`)
- `max_videos`: Máximo de vídeos a comprobar en el canal (por defecto 20)
- `chunk_duration_minutes`: Duración de cada chunk de audio para transcripción (por defecto 30)
- `isTesting`: Si es `true`, usa un vídeo de prueba fijo en lugar de buscar en el canal
- `test_video_url`: URL del vídeo de prueba (activo solo con `isTesting=true`)

### 2. `congress_youtube_chapter_uploader`
**Fichero:** `congress_videos/youtube_upload_dag.py`

Consulta la vista `uploadable_chapters` de la base de datos, genera metadatos con IA, crea miniaturas, extrae los fragmentos de vídeo con ffmpeg y los sube a YouTube.

**Schedule:** `0 12 * * *` (12:00 cada día)
**Parámetros:**
- `max_chapters`: Máximo de capítulos a subir por ejecución (por defecto 5)
- `min_relevance_score`: Puntuación mínima para subir (por defecto 2, escala 0-5)
- `isTesting`: Siempre `false` en producción para que los vídeos sean públicos

### 3. `generic_youtube_uploader`
**Fichero:** `utils/youtube_uploader_dag.py`

DAG genérico de subida a YouTube. No tiene schedule; solo se lanza cuando otro DAG lo dispara mediante `trigger_dag_api`. Acepta configuración vía `dag_run.conf` con la lista de vídeos a subir.

### 4. `git_sync_dag`
**Fichero:** `utils/git_sync_dag.py`

Sincroniza manualmente el repositorio de DAGs desde GitHub. Solo se lanza de forma manual desde la UI de Airflow o vía CLI. Reemplaza el contenedor continuo de git-sync.

---

## Dependencias y tecnologías

| Categoría | Tecnología | Versión |
|-----------|-----------|---------|
| Orquestación | Apache Airflow | 2.10.2 |
| Base de datos | PostgreSQL | 16 |
| Lenguaje | Python | 3.11+ |
| Contenedores | Docker + Compose | Latest |
| Descarga de vídeo | yt-dlp + pytubefix | Latest |
| Transcripción | openai-whisper (local) + Docker ASR API | Latest |
| IA / LLM | OpenAI GPT-4o-mini / GPT-3.5-turbo | API |
| YouTube API | google-api-python-client | Latest |
| Imágenes | Pillow | Latest |
| PDF | PyPDF2 | Latest |
| HTML scraping | BeautifulSoup4 | >=4.12.2 |
| Video processing | ffmpeg | Sistema |
| Autenticación YouTube | google-auth-oauthlib | Latest |

---

## Variables de entorno requeridas

Crear un fichero `.env` en la raíz del repositorio (no se sube a git):

```bash
# Airflow
AIRFLOW__CORE__FERNET_KEY=<clave_generada>
AIRFLOW_DB_NAME=airflow_dev_db          # o airflow_db en producción

# GitHub (para git_sync_dag y clone inicial)
GITHUB_USER=<usuario>
GITHUB_TOKEN=<personal_access_token>
GITHUB_REPO=airflow-dags
GIT_SYNC_BRANCH=dev                     # o main en producción

# OpenAI
OPENAI_API_KEY=<api_key>

# YouTube Data API (para leer canal, no para subir)
YOUTUBE_API_KEY=<api_key>

# PostgreSQL (base de datos de proyecto)
POSTGRES_HOST=postgres_shared
POSTGRES_PORT=5432
POSTGRES_DB=<nombre_db>
POSTGRES_USER=<usuario>
POSTGRES_PASSWORD=<password>
POSTGRES_SCHEMA=development             # o production

# Whisper (opcional, si se usa API Docker)
WHISPER_API_HOST=whisper-api
WHISPER_API_PORT=9000
```

El token OAuth de YouTube (`congress_youtube_token.pickle`) se genera con el script `congress_videos/scripts/generate_youtube_token.py` y se coloca en `/opt/airflow/data/congress_videos/` (montado desde NAS en Docker).

---

## Cómo ejecutar localmente

### Prerrequisitos
- Conda instalado
- Entorno `airflow` creado con las dependencias

```bash
# Crear entorno
conda create -n airflow python=3.11
conda activate airflow
pip install apache-airflow==2.10.2 apache-airflow-providers-postgres \
    openai beautifulsoup4 requests urllib3 google-auth google-auth-oauthlib \
    google-auth-httplib2 google-api-python-client Pillow PyPDF2 \
    yt-dlp pytubefix openai-whisper psycopg2-binary python-dotenv ruff
```

### Validar sintaxis de un DAG
```bash
conda run -n airflow python congress_videos/youtube_channel_monitor_dag.py
```

### Listar DAGs
```bash
conda run -n airflow airflow dags list
```

### Probar un DAG completo
```bash
conda run -n airflow airflow dags test congress_youtube_channel_monitor 2025-01-15
```

### Probar una tarea individual
```bash
conda run -n airflow airflow tasks test congress_youtube_channel_monitor fetch_youtube_channel_videos 2025-01-15
```

### Desplegar con Docker (desarrollo)
```bash
# Crear .env con las variables requeridas, luego:
docker build -t my-airflow:latest .
docker compose up -d
# UI disponible en http://localhost:8081
```

### Desplegar en producción
```bash
docker compose -f docker-compose.prod.yml up -d
# UI disponible en http://localhost:8082
```

### Levantar Whisper ASR (servicio de transcripción)
```bash
docker compose -f docker-compose-whisper.yml up -d
# API disponible en http://localhost:9000
```
