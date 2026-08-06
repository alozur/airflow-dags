# Benchmark de Chonkie sobre SRT parlamentario

## Objetivo

Comprobar si `SemanticChunker` de Chonkie puede producir subtemas semánticos
con timestamps trazables desde un SRT real del Congreso. Esta prueba no intenta
detectar turnos de orador: esa responsabilidad sigue siendo de la diarización.

## Muestra y entorno

- SRT consolidado del vídeo `QQIRmbU7UJ0`, almacenado en el NAS.
- Muestra: primeros 30 minutos, 505 bloques SRT y 18.055 caracteres normalizados.
- Ejecución: equipo local, CPU, SRT leído desde el NAS por SSH. El NAS no fue modificado.
- Modelo: [`intfloat/multilingual-e5-small`](https://huggingface.co/intfloat/multilingual-e5-small).
- Librería: [Chonkie SemanticChunker](https://docs.chonkie.ai/oss/chunkers/semantic-chunker).

## Preparación obligatoria del SRT

El SRT contenía índices numéricos de cue y bloques parcialmente solapados. Antes
de invocar Chonkie, el harness:

1. Ignoró las líneas de índice.
2. Normalizó el texto de cada bloque sin alterar sus timestamps originales.
3. Construyó una cadena canónica y guardó el intervalo de caracteres de cada
   bloque SRT.
4. Alineó secuencialmente cada salida de Chonkie con la cadena canónica.
5. Obtuvo el inicio y final de cada subtema desde el primer y último bloque SRT
   que intersectan ese intervalo.

No se derivan timestamps mediante proporciones de caracteres. Si una salida no
se puede alinear de forma exacta y cronológica, debe fallar y no persistirse.

## Configuraciones probadas

| Configuración | Resultado | Veredicto |
|---|---:|---|
| `threshold=0.75`, `chunk_size=256`, mínimo 2 oraciones, delimitador con `\n` | 144 chunks; mediana muy corta | Rechazada: cada cue SRT se comporta como frontera artificial. |
| `threshold=0.50`, `chunk_size=256`, mínimo 2 oraciones, delimitador con `\n` | 106 chunks; mediana 13,8 s | Rechazada: bajar el umbral no resuelve el problema estructural. |
| `threshold=0.75`, `chunk_size=512`, mínimo 4 oraciones, delimitadores `. `, `! ` y `? ` | 18 chunks; mediana 105,3 s | Baseline para continuar validando. |

La configuración baseline tardó 67,22 s, incluyendo la carga del modelo, sobre
30 min de SRT. Los chunks duraron entre 19,4 y 287 s.

## Observaciones de calidad

- Los límites trazables funcionan: todos los chunks de la baseline se resolvieron
  a bloques y timestamps SRT de origen.
- Eliminar `\n` de los delimitadores es imprescindible. Los saltos de línea de
  subtítulos son detalles de presentación, no finales de oración semánticos.
- La baseline reconoce secciones argumentales de aproximadamente uno a cuatro
  minutos, pero genera algún fragmento corto de menos de 30 segundos.
- Los timestamps de cues pueden solaparse unos segundos. Los `semantic_sections`
  persistidos necesitarán una normalización posterior para quedar ordenados y no
  solaparse.
- Los cambios de orador no coinciden necesariamente con los cambios de tema. Por
  eso Chonkie no puede cortar intervenciones ni reemplazar diarización.
- La transcripción mezcla español y catalán y tiene errores ASR. El modelo siguió
  generando grupos coherentes a grandes rasgos, pero la calidad debe revisarse en
  más de una sesión antes de adoptar el umbral.

## Mejora: texto continuo y fusión temporal-semántica

El SRT se entregó después como texto continuo: los bloques se separaron por un
espacio, no por un salto de línea. Eso evita que el formato visual de un
subtítulo altere la oración que recibe el modelo.

Después de Chonkie, cada sección menor que una duración mínima se fusionó con
su vecino cronológico de mayor similitud coseno. La similitud se calcula con el
mismo modelo de embeddings; no se fusiona por cercanía temporal sin comprobar
el contenido.

| Duración mínima posterior | Secciones finales | Mediana | Mínimo | Máximo |
|---|---:|---:|---:|---:|
| Sin fusión | 22 | 67,2 s | 19,4 s | 311,6 s |
| 60 s | 13 | 106,8 s | 62,9 s | 311,6 s |
| 90 s | 9 | 195,5 s | 106,8 s | 387,1 s |

El baseline recomendado para nuevas muestras es 90 s: en esta sesión equivale a
unas nueve secciones en 30 minutos, con duraciones de aproximadamente dos a
seis minutos. Es una configuración de benchmark, no una decisión permanente;
debe validarse sobre sesiones con debate rápido y preguntas-respuestas.

## Ejecución completa del vídeo

La misma configuración se ejecutó sobre los `06:01:55` del SRT completo del
vídeo fuente. Procesó 7.670 bloques y produjo 414 chunks crudos, reducidos a
106 secciones tras fusionar las inferiores a 90 s. La mediana final fue 172 s;
el proceso local tardó 231,87 s.

El informe con cada rango, vista previa y enlace de YouTube al timestamp de
inicio está en [CHONKIE_FULL_VIDEO_CHUNKS.md](CHONKIE_FULL_VIDEO_CHUNKS.md).
El visor interactivo está en [chonkie-full-video-chunks.html](chonkie-full-video-chunks.html).
Estas secciones siguen siendo subtemas semánticos experimentales: se solapan
ligeramente en tiempo cuando los cues SRT se solapan y no sustituyen los turnos
de orador que debe producir la diarización.

## Configuración de granularidad gruesa

Como alternativa al resultado de 106 secciones, se probó una jerarquía autónoma
que no consume el orden del día: primero se eliminan fragmentos inferiores a
90 s y después se conservan los cambios semánticos más fuertes hasta llegar a
una duración objetivo de 2.000 s por sección. Para esta sesión de seis horas
produjo 11 secciones, con mediana de 2.724,4 s.

Configuración: `threshold=0.55`, `chunk_size=2048`, ventana de similitud 8,
filtro 11 y mínimo de 8 oraciones. El objetivo de 2.000 s está calibrado para
esta prueba, no infiere ni reproduce el orden del día: debe configurarse o
derivarse de una política de duración antes de usarlo en producción.

## Configuración candidata

```python
from chonkie import SemanticChunker
from chonkie.embeddings import SentenceTransformerEmbeddings

embeddings = SentenceTransformerEmbeddings(
    "intfloat/multilingual-e5-small",
    device="cpu",
)

chunker = SemanticChunker(
    embedding_model=embeddings,
    threshold=0.75,
    chunk_size=512,
    similarity_window=2,
    min_sentences_per_chunk=4,
    min_characters_per_sentence=1,
    delim=[". ", "! ", "? "],
    skip_window=0,
)
```

Después de obtener los chunks, fusionar iterativamente los que duren menos de
`min_semantic_section_seconds=90` con el vecino anterior o posterior de mayor
similitud coseno. Los rangos finales se normalizan para que sean cronológicos y
no solapados, usando únicamente timestamps de bloques SRT de origen.

## Recomendación

Adoptar Chonkie como capa de subtemas, no como sustituto directo de diarización.
Antes de integrarlo en el DAG:

1. Repetir el benchmark sobre varias sesiones y revisar manualmente límites.
2. Comparar al menos los umbrales `0.70`, `0.75` y `0.80` con el mismo
   preprocesado.
3. Fusionar después de Chonkie los subtemas demasiado cortos con el vecino
   cronológico de mayor similitud semántica. Empezar con 90 s y comparar 60/90 s
   en sesiones adicionales.
4. Normalizar rangos SRT solapados sin inventar contenido.
5. Ejecutar la misma muestra en el NAS antes de habilitarlo en Airflow.
