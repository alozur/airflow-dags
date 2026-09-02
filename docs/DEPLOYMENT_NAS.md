# Guía de Despliegue en NAS (Synology / QNAP)

> Última actualización: 2026-09-02
> Stack: Apache Airflow 2.11.1 · PostgreSQL 16 · Whisper ASR · Docker Compose

---

## Índice

1. [Arquitectura del despliegue](#1-arquitectura-del-despliegue)
2. [Requisitos del NAS](#2-requisitos-del-nas)
3. [Prerequisitos externos](#3-prerequisitos-externos)
4. [Estructura de directorios en el NAS](#4-estructura-de-directorios-en-el-nas)
5. [Variables de entorno](#5-variables-de-entorno)
6. [Construcción de la imagen Docker](#6-construcción-de-la-imagen-docker)
7. [Configuración de redes Docker externas](#7-configuración-de-redes-docker-externas)
8. [Inicialización de la base de datos](#8-inicialización-de-la-base-de-datos)
9. [Despliegue del stack completo](#9-despliegue-del-stack-completo)
10. [Token de YouTube OAuth](#10-token-de-youtube-oauth)
11. [Acceso a la UI de Airflow](#11-acceso-a-la-ui-de-airflow)
12. [Actualización del código (CI/CD)](#12-actualización-del-código-cicd)
13. [Troubleshooting](#13-troubleshooting)
14. [Seguridad](#14-seguridad)

---

## 1. Arquitectura del despliegue

El stack está compuesto por tres grupos de servicios Docker **independientes** que se comunican entre sí mediante redes externas.

```
┌─────────────────────────────────────────────────────────────────┐
│  NAS: /volume1/docker/                                          │
│                                                                 │
│  ┌─── airflow (docker-compose.prod.yml) ───────────────────┐  │
│  │  airflow-webserver   → puerto 8082                       │  │
│  │  airflow-scheduler                                        │  │
│  │  airflow-init        (one-shot: db init + migrate)        │  │
│  │  init-dags           (one-shot: git clone / pull)         │  │
│  │  init-perms          (one-shot: chown volúmenes)          │  │
│  └──────────────────────────────────────────────────────────┘  │
│           │ postgres_infra_network (externa)                    │
│  ┌─── postgres (stack separado) ───────────────────────────┐   │
│  │  postgres_shared     → puerto 5432 (solo red interna)   │   │
│  └──────────────────────────────────────────────────────────┘   │
│           │ whisper_network (externa)                           │
│  ┌─── whisper (docker-compose-whisper.yml) ────────────────┐   │
│  │  whisper-api         → puerto 9000                       │   │
│  └──────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────┘
```

**Flujo de actualización de DAGs:**

1. `init-dags` clona / hace pull del repositorio en GitHub al volumen `git_dags_prod`.
2. El scheduler detecta cambios en `/opt/airflow/dags/repo` cada 120 s.
3. Para actualizaciones manuales: triggear el DAG `git_sync_dag` desde la UI.

---

## 2. Requisitos del NAS

| Recurso | Mínimo | Recomendado |
|---------|--------|-------------|
| RAM | 4 GB | 8 GB |
| CPU | 2 núcleos (x86-64) | 4 núcleos |
| Almacenamiento | 20 GB libres | 100 GB+ (videos) |
| Docker Engine | 20.10+ | última versión |
| Docker Compose | v2 (`docker compose`) | v2.x |

> **Nota sobre ARM:** La imagen base `apache/airflow:2.11.1` soporta ARM64 (Synology con procesadores ARM). `openai-whisper` también funciona en ARM. Verificar compatibilidad de `ffmpeg` en el NAS específico.

**Synology:** Activar Docker desde el Package Center. Requiere DSM 7+ y CPU x86-64 o serie Plus/Enterprise con ARM64.

**QNAP:** Instalar Container Station desde el App Center.

---

## 3. Prerequisitos externos

Antes de desplegar en el NAS se necesitan:

### 3.1 PostgreSQL (instancia compartida)

PostgreSQL corre en un stack Docker **separado** en la misma red `postgres_infra_network`. Si no existe aún:

```bash
# Crear la red externa primero (ver sección 7)
# Luego levantar PostgreSQL con algo como:
docker run -d \
  --name postgres_shared \
  --network postgres_infra_network \
  -e POSTGRES_USER=postgres \
  -e POSTGRES_PASSWORD=<contraseña_segura> \
  -v /volume1/docker/postgres/data:/var/lib/postgresql/data \
  postgres:16
```

Crear los usuarios y bases de datos de Airflow:

```sql
-- Conectar como superusuario (postgres)
CREATE USER airflow WITH PASSWORD 'airflow_password';
CREATE DATABASE airflow_db OWNER airflow;
```

> El valor exacto de `airflow_password` debe coincidir con la cadena en `AIRFLOW__DATABASE__SQL_ALCHEMY_CONN` del docker-compose.

### 3.2 Cuenta de GitHub con token PAT

Necesario para que `init-dags` pueda clonar el repositorio privado.

- Crear un **Personal Access Token** (PAT) en GitHub → Settings → Developer settings → Tokens (classic)
- Permisos mínimos: `repo` (acceso completo a repositorios privados)

### 3.3 Claves de API externas

| Servicio | Variable | Dónde obtenerla |
|----------|----------|-----------------|
| OpenAI | `OPENAI_API_KEY` | https://platform.openai.com/api-keys |
| YouTube Data API v3 | `YOUTUBE_API_KEY` | https://console.cloud.google.com/apis/credentials |
| YouTube OAuth (upload) | Token `.pickle` | Generado localmente (ver sección 10) |

---

## 4. Estructura de directorios en el NAS

Crear esta estructura **antes** de levantar el stack:

```
/volume1/docker/airflow/
├── .env                          ← Variables de entorno (secreto, no commitear)
├── congress_videos/              ← Datos persistentes montados en el contenedor
│   ├── congress_youtube_token.pickle  ← Token OAuth de YouTube (ver sección 10)
│   ├── assets/
│   │   ├── congress_chamber_background.png
│   │   ├── congress_channel_logo.png
│   │   └── fonts/
│   │       ├── LiberationSans-Bold.ttf
│   │       └── LiberationSans-Regular.ttf
│   ├── videos/                   ← Videos descargados del Congreso
│   └── downloads/                ← Descargas de YouTube channel monitor
│       └── {fecha}/
│           └── {video_id}/
│               ├── audio_chunks/
│               └── srt_files/
```

Comandos para crear la estructura:

```bash
mkdir -p /volume1/docker/airflow/congress_videos/{assets/fonts,videos,downloads} && \
touch /volume1/docker/airflow/congress_videos/congress_youtube_token.pickle
```

También se necesita el directorio para Whisper:

```bash
mkdir -p /volume1/docker/whisper/audio
```

---

## 5. Variables de entorno

Crear el fichero `/volume1/docker/airflow/.env` con el siguiente contenido. **Nunca commitear este fichero.**

```bash
# ─────────────────────────────────────────
# AIRFLOW — Core
# ─────────────────────────────────────────

# Clave Fernet para cifrado interno de credenciales
# Generar con: python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
AIRFLOW__CORE__FERNET_KEY=<clave_fernet_generada>

# Base de datos de Airflow (prod usa airflow_db, dev usa airflow_dev_db)
AIRFLOW_DB_NAME=airflow_db

# ─────────────────────────────────────────
# GITHUB — Sincronización de DAGs
# ─────────────────────────────────────────
GITHUB_USER=<tu_usuario_github>
GITHUB_TOKEN=<tu_pat_github>
GITHUB_REPO=airflow-dags
GIT_SYNC_BRANCH=main

# ─────────────────────────────────────────
# OPENAI
# ─────────────────────────────────────────
OPENAI_API_KEY=sk-...

# ─────────────────────────────────────────
# YOUTUBE DATA API
# ─────────────────────────────────────────
YOUTUBE_API_KEY=AIza...

# ─────────────────────────────────────────
# PIKZELS — Thumbnail generation API
# ─────────────────────────────────────────
PIKZELS_API_KEY=pkz_...

# ─────────────────────────────────────────
# POSTGRESQL — Base de datos de proyecto
# ─────────────────────────────────────────
POSTGRES_HOST=postgres_shared
POSTGRES_PORT=5432
POSTGRES_DB=<nombre_base_de_datos_del_proyecto>
POSTGRES_USER=airflow
POSTGRES_PASSWORD=airflow_password
POSTGRES_SCHEMA=production
```

> **Notas importantes:**
> - `POSTGRES_HOST=postgres_shared` es el nombre del contenedor PostgreSQL en la red `postgres_infra_network`.
> - `POSTGRES_SCHEMA` puede ser `production` o `development` según el entorno.
> - La contraseña `airflow_password` de `SQL_ALCHEMY_CONN` está hardcodeada en el docker-compose. Cambiarla allí también si se usa una contraseña diferente.

---

## 6. Construcción de la imagen Docker

Las imágenes son **específicas por entorno** desde issue #255: `my-airflow:dev` (usada por `docker-compose.yml`) y `my-airflow:prod` (usada por `docker-compose.prod.yml`). Ya no existe una etiqueta `:latest` compartida — un contenedor de prod que se reinicie sin ninguna acción de despliegue siempre resuelve `my-airflow:prod`, nunca un build de dev no validado.

Construir la imagen de **dev** localmente en el NAS a partir del `Dockerfile` del repositorio:

```bash
# Clonar/actualizar el repositorio en el NAS (solo para construir la imagen)
cd /tmp && git clone https://<GITHUB_USER>:<GITHUB_TOKEN>@github.com/<GITHUB_USER>/airflow-dags.git && \
cd airflow-dags && docker build -t my-airflow:dev .
```

Promover a **prod** es un `docker tag`, nunca un segundo build — la imagen es idéntica bit a bit (base + paquetes apt únicamente; el código de los DAGs llega vía el volumen `init-dags`, no la imagen), y evita drift de un `apt-get` re-ejecutado o un segundo build lento en el NAS:

```bash
docker tag my-airflow:dev my-airflow:prod
```

La imagen añade sobre `apache/airflow:2.11.1`:
- `ffmpeg` — corte de vídeo y procesamiento de audio
- `nodejs` + `npm` — soporte yt-dlp / descarga de YouTube
- `git` — necesario para el DAG `git_sync_dag`

Los paquetes Python (openai, yt-dlp, whisper, etc.) se instalan en el primer arranque vía `_PIP_ADDITIONAL_REQUIREMENTS`. Esto ralentiza el primer inicio (~5-10 min según velocidad de red), pero permite actualizar paquetes sin reconstruir la imagen. Nótese que esta instalación pip **no está pineada** y se repite en cada arranque del contenedor — ver la nota de verificación post-despliegue en la sección 9.4.

> **Rollback de emergencia:** tras el corte a etiquetas por entorno, la imagen `my-airflow` con la etiqueta huérfana previa (`latest`, build de Airflow 2.10.2) que queda en el NAS es el artefacto de rollback de coste cero. **No purgarla** hasta confirmar que tanto dev como prod están estables en 2.11.1 (ver sección 9.4).

---

## 7. Configuración de redes Docker externas

Las redes externas deben crearse **antes** de levantar cualquier stack.

```bash
docker network create postgres_infra_network && docker network create whisper_network
```

Verificar que existen:

```bash
docker network ls | grep -E "postgres_infra|whisper"
```

---

## 8. Inicialización de la base de datos

### 8.1 Base de datos de Airflow

La inicialización la hace automáticamente el servicio `airflow-init` al primer `docker compose up`. No es necesaria ninguna acción manual.

### 8.2 Schema del proyecto (production)

Ejecutar el schema SQL **una vez** antes de que los DAGs empiecen a escribir datos. Los ficheros están en el repositorio clonado en el paso anterior:

```bash
psql -h <ip_nas> -p 5432 -U postgres -d <POSTGRES_DB> \
  -f /tmp/airflow-dags/congress_videos/sql/production_schema.sql && \
psql -h <ip_nas> -p 5432 -U postgres -d <POSTGRES_DB> \
  -f /tmp/airflow-dags/congress_videos/sql/grant_permissions_production.sql
```

---

## 9. Despliegue del stack completo

### 9.1 Levantar Whisper ASR (opcional pero recomendado)

```bash
cd /volume1/docker/airflow && docker compose -f docker-compose-whisper.yml up -d
```

Esto levanta `whisper-api` en el puerto `9000` usando el modelo `tiny` (recomendado para NAS con 4 GB RAM). Para cambiar el modelo editar `ASR_MODEL` en el compose:
- `tiny` → ~150 MB RAM, menor precisión
- `base` → ~500 MB RAM
- `small` → ~1.5 GB RAM, buena precisión

### 9.2 Levantar Airflow (producción)

```bash
cd /volume1/docker/airflow && docker compose -f docker-compose.prod.yml --env-file .env up -d
```

**Orden de arranque automático (manejado por `depends_on`):**
1. `init-dags` — clona o actualiza el repositorio desde GitHub
2. `init-perms` — ajusta propietario (UID 50000 = usuario `airflow`) y permisos
3. `airflow-init` — inicializa/migra la BD de Airflow
4. `airflow-webserver` y `airflow-scheduler` — arrancan en paralelo

### 9.3 Verificar que todo está corriendo

```bash
docker compose -f docker-compose.prod.yml ps && \
docker logs airflow-webserver-prod --tail=30 && \
docker logs airflow-scheduler-prod --tail=30
```

El webserver tarda ~60 s en estar disponible (healthcheck con `start_period: 60s`).

### 9.4 Runbook de seguridad para migraciones de la BD de metadatos (issue #255)

Aplica a cualquier bump del núcleo de Airflow que implique una migración de esquema (p. ej. 2.10.2 → 2.11.1). El **superusuario de `postgres_shared` es `admin`, no `postgres`**.

**Ventana recomendada: 14:00–20:00 UTC.** `postgres_shared` tiene `max_connections=100` compartidas entre seis stacks (focusgrove, habits, aurum, gym_app, n8n, airflow); tanto el `pg_dump` como el `recreate` del stack añaden presión de conexiones.

#### Paso 1 — Backup verificado (obligatorio, antes de cualquier `up`)

Por stack, antes de desplegar la nueva imagen en ese entorno:

```bash
ssh nas
/usr/local/bin/docker exec postgres_shared pg_dump -U admin -Fc -d airflow_dev_db -f /tmp/airflow_dev_db_pre2111.dump
/usr/local/bin/docker exec postgres_shared pg_restore --list /tmp/airflow_dev_db_pre2111.dump | head
/usr/local/bin/docker cp postgres_shared:/tmp/airflow_dev_db_pre2111.dump /volume1/docker/backups/
ls -l /volume1/docker/backups/airflow_dev_db_pre2111.dump   # debe ser distinto de cero
```

(sustituir `airflow_dev_db` por `airflow_db` para prod). **Gate obligatorio: NO continuar a `up` — y sobre todo NO ejecutar ningún `DROP DATABASE`** — hasta que el fichero tenga tamaño no-cero **y** `pg_restore --list` complete sin error.

#### Paso 2 — Orden de despliegue (no negociable — construir la imagen ANTES de mergear)

Un cambio en `docker-compose.yml`/`docker-compose.prod.yml` **no** llega al NAS por `git_sync` (ver sección 12.1) — requiere pegar el compose actualizado en Portainer y redeploy manual, o `docker compose up` por CLI. Si ese redeploy referencia `my-airflow:dev` y la imagen todavía no existe, el stack cae. El orden siguiente es seguro tanto si Portainer está en modo *Repository* (auto-redeploy) como en modo *paste* (manual):

```
Dev:  1. docker build -t my-airflow:dev .         (desde la rama del PR, en el NAS)
      2. Paso 1 de este runbook (pg_dump airflow_dev_db + pg_restore --list)
      3. merge del PR a dev
      4. docker compose -f docker-compose.yml --env-file .env up -d
      5. Checks de la sección "Paso 3" de abajo + `airflow dags list-import-errors` vacío

Prod: 1. docker tag my-airflow:dev my-airflow:prod   (promoción, sin rebuild)
      2. Paso 1 de este runbook (pg_dump airflow_db)
      3. merge de dev a main
      4. docker compose -f docker-compose.prod.yml --env-file .env up -d
      5. Checks de la sección "Paso 3" de abajo + 0 errores de importación
```

`git_sync_dag` tiene `schedule=None` (`utils/git_sync_dag.py:32`) — nunca se dispara por cron. El vector real de refresco de código es `init-dags`, que hace `git reset --hard origin/<rama>` en cada `up` — código y reinstalación de `_PIP_ADDITIONAL_REQUIREMENTS` llegan juntos en el mismo `up`.

#### Paso 3 — Verificación post-`up` (exit 0 NO es evidencia suficiente)

**Hallazgo crítico:** el comando de `airflow-init` (`docker-compose.yml:202-209` / equivalente en prod) es `bash -c` **sin `set -e`** — dos comandos consecutivos (`airflow db init` y `airflow db migrate`) en el mismo script. Si `airflow db init` falla, el script sigue y solo el exit status de `airflow db migrate` se propaga. Un `airflow-init` con exit 0 **no prueba** que ambos pasos fueron limpios. Ejecutar explícitamente:

```bash
docker inspect --format '{{.State.ExitCode}}' airflow-init-dev      # debe ser 0
docker logs airflow-init-dev 2>&1 | grep -iE 'error|traceback'      # debe estar vacío
docker exec airflow-scheduler-dev airflow db check-migrations -t 0
docker exec airflow-scheduler-dev airflow version                   # debe leer 2.11.1
```

(sustituir `-dev` por `-prod` para el stack de producción). El check de `airflow version` **no es cosmético**: `_PIP_ADDITIONAL_REQUIREMENTS` reinstala paquetes Python **sin pinear** en cada arranque del contenedor, y en teoría podría arrastrar el núcleo de Airflow fuera del pin del `Dockerfile`. Es el único check que confirma que el núcleo en ejecución es el que el `Dockerfile` fijó.

#### Rollback

**Camino autoritativo: restaurar desde el `pg_dump` + revertir la etiqueta de imagen** — no `airflow db downgrade`.

```
1. docker compose down
2. DROP DATABASE <airflow_dev_db|airflow_db>; CREATE DATABASE <...>;
3. pg_restore -U admin -d <airflow_dev_db|airflow_db> /volume1/docker/backups/<dump-verificado>
4. git revert <commit(s) de este cambio> en el repo del NAS (o checkout de la rama previa)
5. docker compose up -d
```

Tras revertir el compose, la imagen resuelta vuelve a ser `my-airflow` con la etiqueta previa a este cambio (`latest`, build de Airflow 2.10.2) — **sigue existiendo en el NAS**, así que el rollback no requiere rebuild. No purgarla hasta que prod esté verificado estable en 2.11.1.

`airflow db downgrade` se documenta como **oportunista únicamente, no como camino primario**: debe ejecutarse desde la imagen 2.11 de la que se está haciendo rollback (trampa de orden), y las revisiones de downgrade de Alembic están mucho menos probadas que las de upgrade — no puede recuperar de una migración parcialmente aplicada. El `pg_dump` verificado en el Paso 1 es en lo que se apoya este plan.

---

## 10. Token de YouTube OAuth

Los DAGs de subida de vídeos (`youtube_upload_dag`) necesitan un token OAuth para la YouTube Data API. Este token **no puede generarse dentro de Docker** porque requiere un navegador web.

**Pasos (ejecutar en una máquina local con Python y navegador):**

```bash
# 1. Instalar dependencias
pip install google-auth google-auth-oauthlib google-api-python-client

# 2. Descargar client_secrets.json desde Google Cloud Console
#    https://console.cloud.google.com/apis/credentials
#    Crear credencial OAuth 2.0 → Aplicación de escritorio → Descargar JSON
#    Guardar como congress_videos/client_secrets.json

# 3. Generar el token (abrirá el navegador)
cd /ruta/al/repo/airflow-dags
python congress_videos/scripts/generate_youtube_token.py

# 4. Copiar el token generado al NAS
scp congress_videos/congress_youtube_token.pickle \
    user@nas:/volume1/docker/airflow/congress_videos/congress_youtube_token.pickle
```

El token se auto-refresca. El fichero debe tener permisos `666` (el servicio `init-perms` lo configura automáticamente en cada arranque del stack).

**Renovación:** Si el token expira sin refresh_token válido, repetir el paso 3.

---

## 11. Acceso a la UI de Airflow

| Entorno | URL | Puerto host |
|---------|-----|-------------|
| Producción (`docker-compose.prod.yml`) | `http://<ip_nas>:8082` | 8082 → 8080 |
| Desarrollo (`docker-compose.yml`) | `http://<ip_nas>:8081` | 8081 → 8080 |

**Crear el primer usuario admin:**

```bash
docker exec -it airflow-webserver-prod airflow users create \
  --username admin \
  --firstname Admin \
  --lastname User \
  --role Admin \
  --email admin@example.com \
  --password <contraseña_segura>
```

**Acceso desde fuera de la red local:** El puerto solo está mapeado en la interfaz del host. Para acceso externo, configurar un reverse proxy (Nginx Proxy Manager, Traefik) con HTTPS. No exponer el puerto 8082 directamente a Internet sin autenticación adicional.

---

## 12. Actualización del código (CI/CD)

### Al reiniciar el stack

Cada vez que se hace `docker compose up`, el servicio `init-dags` hace un `git reset --hard origin/main` antes de que arranquen webserver y scheduler. El código siempre estará en la última versión de `main`.

### Manual desde la UI de Airflow

Triggear el DAG `git_sync_dag` desde la UI:
1. Ir a `http://<ip_nas>:8082`
2. DAGs → `git_sync_dag` → Trigger DAG

Este DAG hace `git fetch` + `git reset --hard origin/main` y muestra el commit resultante en los logs.

### Manual vía CLI

```bash
docker exec airflow-scheduler-prod \
  bash -c "cd /opt/airflow/dags/repo && git reset --hard origin/main && git log -1 --oneline"
```

### Flujo de branches

```
feature/* → dev → main (PR)
                    ↓
              GitHub Action: sync main → dev (automático al cerrar el PR)
                    ↓
              NAS: init-dags pull desde main (al restart o git_sync_dag)
```

El workflow `.github/workflows/github_sync_main_development.yml` hace merge fast-forward de `main` a `dev` automáticamente al cerrar un PR en `main`.

---

## 12.1 Canales de despliegue: git_sync vs Portainer (#203 / #207 / #215)

El despliegue en el NAS usa **dos canales independientes**. Confundirlos es la causa más común de "hice el cambio pero no se aplicó":

| Canal | Qué gobierna | Cómo se aplica |
|-------|--------------|-----------------|
| **git_sync** | El código de las DAGs (`congress_videos/`, `utils/`, etc.) | Automático: `init-dags` hace `git reset --hard` en cada `docker compose up`; manual: triggear `git_sync_dag` desde la UI o CLI |
| **Portainer** | La topología del stack (`docker-compose.yml` / `docker-compose.prod.yml`) y las variables de entorno (`.env`) | Manual: pegar el contenido actualizado del compose en el stack de Portainer (modo *paste* o *Repository*) y redeploy |

Un cambio en `docker-compose.yml` (redes, roles de base de datos, `dns:`, nombres de contenedor) **nunca** llega al NAS por `git_sync` — ese canal solo actualiza el checkout dentro del volumen `git_dags`, no la definición del stack que ejecuta Docker Compose. Ese tipo de cambio requiere pegar el fichero actualizado en Portainer y volver a desplegar el stack correspondiente.

### Checklist de cutover (roles de base de datos #203 + PAT de GitHub #207)

Los scripts de `congress_videos/sql/grant_permissions*.sql` son **aditivos**: crean roles nuevos sin tocar los permisos ni el `LOGIN` del rol legado `airflow`. El corte a los roles nuevos es un procedimiento manual, en este orden:

1. **Merge + git_sync** — sin cambio de comportamiento todavía (URL de git sin token + `askpass`; las variables de migración siguen sin definir, por lo que se sigue usando el rol legado `airflow`).
2. **Superusuario en Postgres del NAS**: ejecutar ambos scripts de grants (`grant_permissions.sql` en `development`, `grant_permissions_production.sql` en `production`) y después `ALTER ROLE <rol> PASSWORD '<valor-del-vault>';` para `airflow_dev`, `airflow_prod` y `airflow_migrations`.
3. **Stack Portainer de dev**: configurar `POSTGRES_USER=airflow_dev` y `MIGRATION_POSTGRES_USER`/`MIGRATION_POSTGRES_PASSWORD`; redeploy; verificar que `run_migrations` y un DAG con escritura a base de datos corren limpios.
4. **Stack Portainer de prod**: repetir el paso anterior con `airflow_prod`; verificar.
5. **Solo después de verificar dev y prod**: ejecutar el `REVOKE` acotado a esquema (únicamente `development` y `production`) sobre los grants legados de `airflow`. **No** ejecutar `ALTER ROLE airflow NOLOGIN` ni tocar las bases de datos de metadatos `airflow_db`/`airflow_dev_db` — `airflow` sigue siendo el rol de login de la base de datos de metadatos de Airflow (`SQL_ALCHEMY_CONN`, sin cambios en este proceso).
6. **GitHub**: reemplazar el PAT clásico `repo` por un token *fine-grained* con permiso `Contents: Read-only`; actualizar `GITHUB_TOKEN` en ambos stacks de Portainer.

### `CREATE ON SCHEMA` para el rol owner (issue #310)

`_assume_owner_role` (`utils/migrations_dag.py`) hace `SET ROLE` al rol owner del esquema (`airflow_dev`/`airflow_prod`) antes de ejecutar cada migración, y desde el 2026-09-01 valida además, con `has_schema_privilege(rol, esquema, 'CREATE')`, que ese rol puede crear objetos en su propio esquema. Si el `GRANT CREATE ON SCHEMA` todavía no se aplicó en vivo, el `SET ROLE` sí tiene éxito pero la validación falla con un `MigrationPrivilegeError` explícito (rol, esquema y el `GRANT` exacto a ejecutar) en vez de fallar de forma opaca a mitad de la migración.

Esto es un **prerequisito de merge, no una consecuencia**: el runner no puede corregir sus propios privilegios, así que el `GRANT` debe aplicarse en vivo, con superusuario, en ambos stacks, **antes** de disparar `run_migrations` tras este cambio (paso 2 del checklist de arriba ya lo incluye — `grant_permissions.sql`/`grant_permissions_production.sql` traen ahora `GRANT CREATE ON SCHEMA {esquema} TO {rol_owner};`). En un rebuild completo del stack, repetir este `GRANT` en el mismo paso en que se corren los demás scripts de grants.

Verificación tras aplicar el `GRANT`:

```sql
SELECT has_schema_privilege('airflow_dev', 'development', 'CREATE');   -- development
SELECT has_schema_privilege('airflow_prod', 'production', 'CREATE');   -- production
```

Ambas consultas deben devolver `t` antes de disparar `run_migrations` en el stack correspondiente.

Antes de aplicar cualquier compose actualizado en Portainer, confirmar que renderiza correctamente:

```bash
docker compose -f docker-compose.yml config
docker compose -f docker-compose.prod.yml config
```

---

## 13. Troubleshooting

### Los DAGs no aparecen en la UI

```bash
# Verificar que el repositorio se clonó correctamente
docker exec airflow-scheduler-prod ls /opt/airflow/dags/repo/

# Ver errores de importación de DAGs
docker exec airflow-scheduler-prod airflow dags list-import-errors
```

### Error de conexión a PostgreSQL

```bash
# Verificar que postgres_shared está en la red correcta
docker network inspect postgres_infra_network | grep postgres_shared

# Probar conexión desde el contenedor de Airflow
docker exec airflow-scheduler-prod \
  python -c "import psycopg2; psycopg2.connect(host='postgres_shared', port=5432, dbname='airflow_db', user='airflow', password='airflow_password'); print('OK')"
```

### El webserver no arranca (error Fernet key)

El `AIRFLOW__CORE__FERNET_KEY` está vacío o es inválido. Verificar:

```bash
docker exec airflow-webserver-prod env | grep FERNET
```

Generar una clave nueva:

```bash
docker run --rm apache/airflow:2.11.1 \
  python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```

### `init-perms` falla con "No such file or directory"

El directorio `/volume1/docker/airflow/congress_videos` no existe en el NAS:

```bash
mkdir -p /volume1/docker/airflow/congress_videos/{assets/fonts,videos,downloads} && \
touch /volume1/docker/airflow/congress_videos/congress_youtube_token.pickle
```

### Whisper API no responde

El código tiene fallback automático: si el API Docker no está disponible, usa la librería `openai-whisper` instalada en el contenedor de Airflow. Para diagnosticar:

```bash
docker logs whisper-api --tail=30
docker exec airflow-scheduler-prod curl -sf http://whisper-api:9000/ && echo "OK" || echo "NO DISPONIBLE"
```

### Los paquetes Python tardan mucho al arrancar

Normal en el **primer arranque** (instalan ~10 paquetes via pip). Para acelerar arranques futuros, añadir los paquetes al `Dockerfile` y reconstruir la imagen. Monitorear progreso:

```bash
docker logs airflow-scheduler-prod -f | grep -iE "pip|installing|error"
```

### El token de YouTube da error 401/403

El token pickle ha expirado. Regenerarlo (ver sección 10) y copiarlo al NAS:

```bash
scp congress_youtube_token.pickle user@nas:/volume1/docker/airflow/congress_videos/ && \
docker restart airflow-scheduler-prod
```

### `init-dags` falla con error de autenticación en GitHub

Verificar que `GITHUB_TOKEN` está correctamente configurado en `.env` y que el token tiene permiso `repo`:

```bash
docker logs airflow-init-dags-prod
```

---

## 14. Seguridad

### Credenciales — qué nunca debe estar en git

| Fichero | Motivo |
|---------|--------|
| `.env` | Contiene todas las claves de API y contraseñas |
| `congress_youtube_token.pickle` | Token OAuth de YouTube |
| `client_secrets.json` | Credenciales OAuth de Google Cloud |
| `*youtube_token*.pickle` | Cualquier variante del token |

Todos están en `.gitignore`. Verificar con `git status` antes de cualquier commit.

### Puertos expuestos

| Puerto | Servicio | Recomendación |
|--------|---------|---------------|
| 8082 | Airflow UI (prod) | Solo LAN o detrás de reverse proxy con HTTPS |
| 8081 | Airflow UI (dev) | Solo LAN |
| 9000 | Whisper API | Solo red interna Docker — no mapear al host |
| 5432 | PostgreSQL | Solo red interna Docker — no mapear al host |

### Fernet Key

La `AIRFLOW__CORE__FERNET_KEY` cifra las credenciales almacenadas en la BD de Airflow (contraseñas de conexiones, tokens). Guardar esta clave en un gestor de contraseñas. Si se pierde, las conexiones almacenadas en Airflow deberán reconfigurarse manualmente.

### Usuarios de Airflow

Airflow tiene RBAC activado por defecto. Crear usuarios con el rol mínimo necesario (`Viewer`, `User`, `Op`, `Admin`). No compartir el usuario `admin`.

### Actualizaciones de seguridad

Seguir el orden de la sección 9.4 (backup verificado antes de `up`, dev antes de prod). Para una actualización periódica sin cambio de esquema:

```bash
# Actualizar imagen base periódicamente (dev primero)
docker pull apache/airflow:2.11.1 && \
cd /tmp/airflow-dags && git pull && docker build -t my-airflow:dev . && \
cd /volume1/docker/airflow && docker compose -f docker-compose.yml up -d
# Verificar dev (sección 9.4, Paso 3), luego promover a prod:
docker tag my-airflow:dev my-airflow:prod && \
cd /volume1/docker/airflow && docker compose -f docker-compose.prod.yml up -d
```

---

## Referencia rápida de comandos

```bash
# Levantar todo el stack
cd /volume1/docker/airflow && \
docker compose -f docker-compose-whisper.yml up -d && \
docker compose -f docker-compose.prod.yml --env-file .env up -d

# Parar todo el stack
cd /volume1/docker/airflow && \
docker compose -f docker-compose.prod.yml down && \
docker compose -f docker-compose-whisper.yml down

# Ver logs en tiempo real
docker compose -f docker-compose.prod.yml logs -f

# Reiniciar solo el scheduler
docker restart airflow-scheduler-prod

# Forzar actualización de DAGs desde GitHub
docker exec airflow-scheduler-prod bash -c "cd /opt/airflow/dags/repo && git reset --hard origin/main && git log -1 --oneline"

# Crear usuario admin en Airflow
docker exec -it airflow-webserver-prod airflow users create \
  --username admin --role Admin \
  --firstname Admin --lastname User \
  --email admin@example.com --password <contraseña>

# Ver DAGs cargados
docker exec airflow-scheduler-prod airflow dags list

# Ver errores de importación de DAGs
docker exec airflow-scheduler-prod airflow dags list-import-errors

# Generar Fernet Key
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"

---

## Airflow Pools — prerequisitos de operaciones

Algunos DAGs requieren pools de Airflow preconfigurados. Los pools **no** se
crean automáticamente en código; deben ser provistos por un administrador antes
de la primera ejecución del DAG correspondiente.

### `nas_ffmpeg` — pool para `speaker_turn_videos` (issue #88)

Limita la concurrencia de tareas ffmpeg a 1 proceso simultáneo para proteger
el I/O del NAS.

```bash
# Crear el pool (ejecutar una sola vez)
docker exec airflow-scheduler-prod \
  airflow pools set nas_ffmpeg 1 "NAS ffmpeg execution slot"

# Verificar
docker exec airflow-scheduler-prod airflow pools list
```

Sin este pool el DAG `speaker_turn_videos` encola indefinidamente.
Ver también el docstring del DAG en `congress_videos/speaker_turn_videos_dag.py`.
```
