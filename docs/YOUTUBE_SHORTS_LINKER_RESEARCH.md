# YouTube Shorts → Vídeo largo: investigación de viabilidad y arquitectura

> **Fecha:** 2026-06-19
> **Estado:** Investigación cerrada · Decisión de arquitectura tomada (vía híbrida automatizada)
> **Origen:** Deep research multi-fuente con verificación adversarial (21 fuentes, 88 claims extraídas, 25 verificadas, 20 confirmadas).

---

## 1. Objetivo

Automatizar para un canal propio: por cada **Short**, leer en su descripción el **vídeo largo relacionado** que el autor referencia, y establecer ese vídeo largo como el **"vídeo relacionado" oficial del Short** (función *"Añadir vídeo relacionado"* / *Related video* de YouTube Studio).

Despliegue previsto: **NAS + self-hosted GitHub Actions runner** disparado cada X tiempo (mismo patrón que el resto de DAGs de este repo).

---

## 2. Veredicto (TL;DR)

| Pieza | ¿Automatizable por API oficial? | Riesgo |
|-------|----------------------------------|--------|
| Listar vídeos del canal | ✅ Sí (`playlistItems.list`) | Ninguno |
| Leer descripciones / identificar Shorts / parsear URL largo | ✅ Sí (`videos.list`) | Ninguno |
| **Establecer el vídeo relacionado del Short** | ❌ **NO existe API** | — |
| Hacer el enlace vía automatización de navegador (Studio) | ⚠️ Técnicamente posible | **Alto: viola ToS, riesgo de suspensión de cuenta** |

**Conclusión:** la única pieza que el proyecto necesita automatizar (el enlace) es justo la que Google **no expone por API**. Solo se puede hacer mediante automatización de la UI de YouTube Studio, lo que **viola los Términos de Servicio** y conlleva riesgo real de suspensión de la cuenta de Google.

---

## 3. Hallazgos verificados

### 3.1 No hay API oficial para el vídeo relacionado `[confianza: alta · voto 3-0]`
- `videos.update` de la **YouTube Data API v3** solo edita metadata estándar: título, descripción, `categoryId`, tags, `status`, localizaciones. **No existe campo de "vídeo relacionado".**
- La función de enlazar Short ↔ vídeo largo es **exclusiva de la UI de YouTube Studio**.
- Fuentes: [videos resource](https://developers.google.com/youtube/v3/docs/videos), [SearchEngineLand](https://searchengineland.com/youtube-enables-linking-shorts-long-form-videos-430655).

### 3.2 La parte de LECTURA sí es API limpia, robusta y barata `[alta · 3-0]`
- `playlistItems.list` sobre el playlist de *uploads* del canal → ~1 unidad/50 vídeos.
- `videos.list` para leer descripciones y metadata → ~1 unidad/50 vídeos.
- **No hay filtro de "Shorts"** en la API → hay que identificarlos por vídeo (p. ej. duración ≤ 3 min + heurísticas).
- **Evitar `search.list`** → cuesta **100 unidades por llamada**; funde la cuota.
- Cuota por defecto: **10.000 unidades/día** (gratis, de sobra para este caso).
- Scope OAuth: `youtube.readonly` (lectura) o `youtube.force-ssl`.
- Fuente: [quota cost](https://developers.google.com/youtube/v3/determine_quota_cost).

### 3.3 Riesgo legal / ToS `[alta · 3-0]`
- Los ToS de YouTube **prohíben el acceso automatizado** salvo vía `robots.txt`, permiso escrito previo, o por ley.
- Google se reserva **suspender o cancelar la cuenta de Google entera** por incumplimiento material o repetido, y terminar canales por violaciones repetidas.
- Las Developer Policies prohíben automatizar views/uploads/comments/likes y el scraping; sanciones hasta reducción de cuota, revocación de API key o terminación de cuenta.
- ⚠️ Matiz: la cobertura concreta de "automatizar la UI de Studio" proviene del **ToS de consumidor**, no de las Developer Policies (una claim más amplia fue refutada 0-3). El puente "automatizar Studio = incumplir ToS" es **interpretativo pero razonable**.
- Fuentes: [ToS](https://www.youtube.com/static?template=terms), [policy](https://support.google.com/youtube/answer/2802168), [developer-policies](https://developers.google.com/youtube/terms/developer-policies).

### 3.4 Estado del arte de automatización de navegador `[alta · 3-0]`
- **`nodriver`** = sucesor oficial de `undetected-chromedriver`. Async, sin Selenium, CDP de bajo nivel, sesión persistente vía fichero de cookies + `user_data_dir`.
- Los anti-bot detectan Puppeteer/Playwright/Selenium vía la señal **`Runtime.enable` de CDP**; `nodriver` evita ese vector concreto.
- ⚠️ El getter de esa técnica **fue parcheado en V8 (mayo 2025)** y CDP sigue siendo detectable por otras señales. Es una **carrera armamentística**: cualquier solución de navegador puede romperse en semanas.
- `Playwright launchPersistentContext` es alternativa más detectable y **no persiste cookies de sesión sin expiry** (bug abierto).
- Fuentes: [nodriver](https://github.com/ultrafunkamsterdam/nodriver), [DataDome](https://datadome.co/threat-research/how-new-headless-chrome-the-cdp-signal-are-impacting-bot-detection/), [Castle](https://blog.castle.io/from-puppeteer-stealth-to-nodriver-how-anti-detect-frameworks-evolved-to-evade-bot-detection/).

### 3.5 Infra NAS + runner `[alta · 3-0]`
- Un self-hosted GitHub Actions runner corre en NAS (Synology Container Manager / Docker) para repos privados sin coste por minuto. Adecuado para uso hobby/personal, no business-critical.
- (Diciembre 2025: GitHub anunció y luego retiró un cargo por minuto en self-hosted; ninguno en vigor a junio 2026.)
- Fuente: [synology-github-runner](https://github.com/andrelin/synology-github-runner).

### 3.6 Mitos REFUTADOS sobre nodriver (no fiarse del marketing)
- ❌ "nodriver bypassa Captcha/Cloudflare/Imperva/hCaptcha" → refutado (1-2).
- ❌ "nodriver elimina CDP por completo / mejora resistencia a WAF" → refutado (0-3).
- ❌ "undetected-chromedriver tiene ~30% de éxito contra DataDome" → refutado (0-3).

---

## 4. Decisión de arquitectura tomada

**Vía elegida: HÍBRIDA AUTOMATIZADA** (el usuario asume el riesgo de ToS de forma informada).

- API oficial para **toda la lectura**.
- Automatización de navegador (`nodriver`) **solo** para el clic final de enlace.
- Diseño orientado a **minimizar la huella y el riesgo de baneo**.

> Alternativa descartada de momento: híbrido semi-manual (la API genera la cola y el enlace se da a mano, riesgo cero). Queda como fallback si la automatización resulta inviable o frágil.

---

## 5. Arquitectura propuesta

```
┌─────────────────── NAS (Docker) ───────────────────┐
│                                                     │
│  [1] READER (API oficial · legal · robusto)         │
│      google-api-python-client + OAuth               │
│      playlistItems.list → videos.list               │
│      parsea descripción → extrae long-form videoId  │
│      escribe → cola.sqlite (pares Short→largo)       │
│                      │                               │
│                      ▼                               │
│  [2] LINKER (nodriver · riesgo ToS · frágil)         │
│      lee cola.sqlite (solo PENDING)                  │
│      abre Studio con sesión persistente (xvfb)       │
│      clic "Añadir vídeo relacionado"                 │
│      marca DONE / FAILED en la cola                  │
│                                                     │
│  Volúmenes: user_data_dir/ · cola.sqlite · creds.enc │
└─────────────────────────────────────────────────────┘
   ▲ disparado por el self-hosted runner cada X horas
```

**Principio de diseño:** desacoplar lo robusto (READER) de lo frágil (LINKER). Si el LINKER se rompe o se apaga, el READER sigue generando la cola → fallback manual inmediato.

---

## 6. Decisiones de mitigación de riesgo (lo crítico)

1. **El login NO se automatiza JAMÁS.** Login manual una vez en local → exportar `user_data_dir` → cifrar con **Fernet** → montar como volumen. El runner reutiliza la sesión, nunca teclea credenciales. Automatizar el login de Google es el principal disparador anti-bot.
2. **`xvfb`, no headless puro.** Studio detecta `headless=new`. Chrome real sobre display virtual dentro del contenedor → menos detectable.
3. **Bajo volumen + ritmo humano.** Disparo 1×/día (o cada varias horas), **3-5 Shorts/tanda máx.**, delays aleatorios, **solo Shorts nuevos**.
4. **OAuth: publicar la app (no dejarla en "testing").** En modo testing el refresh token **caduca a los 7 días**. Publicarla → refresh token estable. Token cifrado (Fernet) en volumen montado; **no en GitHub Secrets** si runner self-hosted.
5. **Kill-switch / fail-safe.** Si el LINKER detecta redirección a login → **NO reintenta login**; aborta, marca la cola y notifica (push/email) para re-exportar sesión a mano.
6. **Idempotencia.** SQLite con estado `PENDING/DONE/FAILED` por Short. El enlace es **one-time por Short** → el grueso se hace una vez, luego solo nuevos.
7. **Hardening de contenedor:** read-only fs, `tmpfs /tmp`, `no-new-privileges`. SQL siempre parametrizado.

---

## 7. Incógnitas abiertas (validar con un SPIKE, no leyendo)

- **¿Cuánto aguanta la sesión de Google reutilizada en nodriver (headful) antes de exigir re-login/2FA?** Sin evidencia concluyente. Determina cada cuánto hay que re-exportar la sesión a mano.
- **DOM exacto de "Añadir vídeo relacionado" en Studio 2026** (selectores, pasos, confirmaciones) y su estabilidad histórica → estima el coste de mantenimiento del scraper.
- **Persistencia segura** del refresh token OAuth y del `user_data_dir` entre ejecuciones (cifrado at-rest, volumen Docker vs secrets).
- ¿Existe algún partner con permiso escrito de YouTube (excepción ToS) que ofrezca este enlace de forma soportada? (No hallado.)

---

## 8. Plan de arranque recomendado

1. **READER primero** (valor inmediato, riesgo cero): API + parseo + cola SQLite.
2. **Spike manual del LINKER en local** (en paralelo): mapear el DOM de Studio y medir la duración de la sesión de Google con nodriver.
3. Con los datos del spike → dimensionar mantenimiento y meter el LINKER en la NAS con todas las mitigaciones del §6.

---

## 9. Fuentes principales

| Fuente | Tipo | Ángulo |
|--------|------|--------|
| [developers.google.com/youtube/v3/docs/videos](https://developers.google.com/youtube/v3/docs/videos) | primaria | API |
| [determine_quota_cost](https://developers.google.com/youtube/v3/determine_quota_cost) | primaria | API |
| [SearchEngineLand — Shorts linking](https://searchengineland.com/youtube-enables-linking-shorts-long-form-videos-430655) | secundaria | API |
| [github.com/ultrafunkamsterdam/nodriver](https://github.com/ultrafunkamsterdam/nodriver) | primaria | Navegador |
| [DataDome — CDP signal](https://datadome.co/threat-research/how-new-headless-chrome-the-cdp-signal-are-impacting-bot-detection/) | primaria | Anti-bot |
| [Castle — nodriver evolution](https://blog.castle.io/from-puppeteer-stealth-to-nodriver-how-anti-detect-frameworks-evolved-to-evade-bot-detection/) | blog | Anti-bot |
| [YouTube ToS](https://www.youtube.com/static?template=terms) | primaria | Legal |
| [Developer Policies](https://developers.google.com/youtube/terms/developer-policies) | primaria | Legal |
| [synology-github-runner](https://github.com/andrelin/synology-github-runner) | primaria | Infra NAS |
