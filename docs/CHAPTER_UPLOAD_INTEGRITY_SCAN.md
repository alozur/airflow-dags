# Escaneo de integridad de chapters pendientes de subir

> Fecha del escaneo: 2026-07-31
> Entorno: NAS (`airflow-scheduler-prod`), DB `production`, vista `uploadable_chapters`
> Contexto: issue [#24](https://github.com/alozur/airflow-dags/issues/24) — cortes AV1 corruptos que suben "exitosos" pero YouTube abandona el procesamiento

---

## Resumen

De los **50 chapters pendientes de subir** (relevance_score >= 2), **43 están sanos** y **7 van a fallar** porque su fuente está corrupta en disco.

| Resultado | Cantidad | Videos afectados |
|---|---|---|
| ✅ OK (subirán bien) | 43 | 8 videos |
| ❌ CORRUPT (fallarán) | 7 | `GMZ5TwfZJHw` (6), `Ig21NWVso10` (1) |

## Cómo se hizo el escaneo

1. Consulta a la vista `production.uploadable_chapters` con `relevance_score >= 2` (el script del repo usa `>= 4` por defecto y quedaba vacío).
2. Para cada chapter se localizó la fuente en disco (misma lógica que `extract_chapters_from_video`: `downloads/{fecha}/{video_id}/`).
3. Se decodificó **solo la ventana exacta del chapter** con `ffmpeg -v error -err_detect ignore_err -ss <start> -to <end> -i <source> -f null -`.
   - Decodificar el video completo (7-10h AV1) tarda horas en el NAS; la ventana por chapter es la granularidad correcta para predecir si el corte fallará.
4. `returncode == 0` y cero líneas de error → OK. Cualquier error de decode → CORRUPT.

## ❌ Chapters que VAN A FALLAR (7)

| chapter_id | video_id | Ventana (s) | Ventana (h:min) | Errores decode |
|---|---|---|---|---|
| 330 | `GMZ5TwfZJHw` | 12–1511 | 00:00–00:25 | 25.468 |
| 331 | `GMZ5TwfZJHw` | 3096–4381 | 00:51–01:13 | 3.100 |
| 329 | `GMZ5TwfZJHw` | 5930–7355 | 01:38–02:02 | 11.447 |
| 332 | `GMZ5TwfZJHw` | 7384–8880 | 02:03–02:28 | 74.687 |
| 333 | `GMZ5TwfZJHw` | 10448–11667 | 02:54–03:14 | 20.427 |
| 334 | `GMZ5TwfZJHw` | 17545–18751 | 04:52–05:12 | 19.607 |
| 451 | `Ig21NWVso10` | 20526–21738 | 05:42–06:02 | 1.169 |

**Nota crítica:** `GMZ5TwfZJHw` tiene TODOS sus 6 chapters pendientes corruptos — el archivo fuente completo sigue dañado en disco. Es uno de los dos videos "known-bad" del issue #24. **Requiere re-descarga completa** (borrar fuente y re-trigger del DAG pasada la ventana de `download_retry_after`, o forzar `is_processed = FALSE`).

## ✅ Chapters que FUNCIONAN (43)

| video_id | Chapters OK | Cantidad |
|---|---|---|
| `eCCoT-UVbRk` | 402, 403, 404, 405, 406, 407, 408, 409, 410, 411, 412, 413 | 12 |
| `pmLyT3dd1hQ` | 311, 312, 313, 314, 315, 316, 317, 318, 319, 320 | 10 |
| `mjUgQQVHYJg` | 262, 263, 264, 265, 266 | 5 |
| `sahjXSGn-Ak` | 497, 498, 499, 500 | 4 |
| `QQIRmbU7UJ0` | 434, 435, 436, 437 | 4 |
| `mlG0VW6guDI` | 417, 419, 420 | 3 |
| `Z6vFVe60VoU` | 276, 277, 278 | 3 |
| `Ig21NWVso10` | 450 | 2 (451 es el corrupto) |
| `60VJoWho4DI` | 291 | 1 |

## Observaciones

- **El fix del issue #24 funciona:** `sahjXSGn-Ak` (el video confirmado corrupto en el issue) ya se re-descargó y sus 4 chapters decodifican limpios.
- `GMZ5TwfZJHw` sigue corrupto en disco — no fue re-descargado.
- El script del repo `congress_videos/scripts/check_pending_source_integrity.py` hace un decode **completo por video** y usa `min_relevance_score=4` por defecto; para replicar este escaneo por-chapter se usó el script ad-hoc `/tmp/check_chapter_windows.py` dentro del contenedor.

## Cómo replicar

```bash
# Dentro del contenedor airflow-scheduler-prod (NAS)
# 1. Copiar el script ad-hoc al contenedor:
#    docker cp /tmp/check_chapter_windows.py airflow-scheduler-prod:/tmp/

# 2. Ejecutar:
python /tmp/check_chapter_windows.py
# Reporta chapters pendientes (relevance>=2), decodifica la ventana de cada uno
# y termina con un resumen OK / NEEDS ATTENTION.

# Alternativa oficial (por video completo, min_score=4):
cd /opt/airflow/dags/repo
python congress_videos/scripts/check_pending_source_integrity.py --min-relevance-score 2
```
