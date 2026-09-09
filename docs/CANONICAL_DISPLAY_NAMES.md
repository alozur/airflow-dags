# Nombres públicos canónicos de políticos

Cómo se acota, revisa y amplía el catálogo curado slug → nombre público
(`congress_videos/catalogs/politician_display_names.v1.json`). Consulta
[`openspec/changes/canonical-politician-display-names/design.md`](../openspec/changes/canonical-politician-display-names/design.md)
para el diseño del módulo y el cableado de los consumidores; este documento
solo cubre lo que cambia después de que esa PR se integre — mantener el
catálogo correcto.

## Propiedad y alcance

El catálogo es una cuestión de **presentación**: decide qué renderiza un
título o una miniatura para un político que ya ha sido identificado. Está
deliberadamente separado de la resolución de identidad — la identidad
canónica de un participante siempre es el `congress_participants.slug`
producido aguas arriba, y ese slug nunca cambia por culpa de este catálogo.
`canonical_display_name` solo mapea un slug ya resuelto a un nombre público
corto preferido; nunca participa en decidir quién es un orador.

El mantenedor de `congress_videos` es el propietario del catálogo. Cualquier
cambio en `politician_display_names.v1.json` necesita una PR, como cualquier
otro cambio de código — no hay un proceso de aprobación aparte.

## El listado inicial y su criterio de selección

El criterio es mecánico y reproducible, no editorial: **un participante entra
en el listado si ha aparecido al menos dos veces entre
`video_chapters.resolved_participant_slug` y
`speaker_turn_videos.resolved_participant_slug`.** El juicio editorial solo
elige la *forma de presentación* para cada slug admitido (por ejemplo, un
apellido a secas frente a un "Nombre Apellido" desambiguado) — nunca decide
quién es admitido.

Puedes recalcular el conjunto de candidatos con:

```sql
SELECT slug, COUNT(*) AS appearances
FROM (
    SELECT resolved_participant_slug AS slug
    FROM video_chapters
    WHERE resolved_participant_slug IS NOT NULL

    UNION ALL

    SELECT resolved_participant_slug AS slug
    FROM speaker_turn_videos
    WHERE resolved_participant_slug IS NOT NULL
) appearances
GROUP BY slug
HAVING COUNT(*) >= 2
ORDER BY appearances DESC;
```

En el momento de redactar esto (2026-09-09), esta consulta devuelve **11
personas de solo 21** que han aparecido alguna vez con un slug resuelto. Los
otros 10 slugs resueltos aparecieron una sola vez y están correctamente
ausentes del catálogo — una sola aparición no es evidencia suficiente de que
una forma corta curada merezca la deuda editorial que crea (ver más abajo).

## Cadencia de revisión

Revisa el catálogo **trimestralmente**, e inmediatamente después de
cualquiera de estos disparadores:

- **Unas elecciones generales.** El liderazgo y la relevancia de los partidos
  pueden cambiar lo suficiente como para que un apellido a secas antes seguro
  (o el juicio editorial detrás de él) deje de ser preciso.
- **Una remodelación de gobierno.** Que cambie la cartera de un ministro
  afecta a si una forma corta sigue leyéndose como inequívoca y vigente.

Ambos disparadores existen porque las formas cortas curadas son **deuda
editorial por diseño**: `full_name` y la regla de subsecuencia de tokens (más
abajo) solo evitan una forma abreviada *inventada*, no pueden detectar que una
forma esté *desactualizada*. Solo una revisión humana detecta eso.

## Cómo añadir o cambiar un mapeo

1. Añade o edita una entrada en
   `congress_videos/catalogs/politician_display_names.v1.json`. Cada entrada
   necesita `participant_slug`, `display_name`, `full_name`, `ambiguous`,
   `selection_note` (indica el número de apariciones y por qué la forma
   corta es segura) y un bloque `provenance` completo (`publisher`,
   `reference_url`, `evidence_note`, `reviewed_on`).
2. El cargador (`congress_videos/modules/politician_display_names.py`)
   impone estos invariantes en el momento de carga — una violación lanza
   `CatalogValidationError` y hace fallar la importación del DAG de forma
   ruidosa:
   - **Regla de subsecuencia de `full_name`**: el `display_name` normalizado
     debe ser una subsecuencia de tokens del `full_name` normalizado. Esto es
     lo que evita mecánicamente inventar una forma abreviada que en realidad
     no se derive del nombre real de la persona — cada token de
     `display_name` debe aparecer, en orden, dentro de `full_name`.
   - **Sin `participant_slug` duplicado** entre todas las entradas — un
     fallo obligatorio.
   - **Sin `display_name` normalizado en colisión** dentro del conjunto
     resoluble (no `ambiguous`) — un fallo obligatorio. Dos personas
     distintas nunca deben renderizar el mismo nombre corto.
3. Ejecuta la suite de tests del catálogo antes de abrir una PR:

   ```
   uv run pytest tests/congress_videos/test_politician_display_names.py
   ```

### Ejemplo resuelto: la colisión de `Rodríguez`

`isabel-rodriguez-garcia` y `javier-rodriguez-palacios` se reducen ambos a un
apellido a secas `Rodríguez` — la regla de colisión rechaza mapear a
cualquiera de los dos como `Rodríguez` a secas. Ambos se desambiguan en su
lugar con un nombre de pila (`Isabel Rodríguez`, `Javier Rodríguez`). Una
tercera persona con el mismo apellido, `jose-antonio-rodriguez-salas`, queda
deliberadamente **fuera** del listado (por debajo del umbral de ≥2
apariciones) y simplemente recae en el comportamiento de nombre completo.
Este es el caso concreto que la regla de colisión y el umbral de apariciones
existen para gestionar — léelo antes de añadir cualquier entrada nueva de
`Rodríguez`, `García` u otro apellido común.

## Advertencia de degradación silenciosa

`canonical_display_name` **nunca lanza** excepciones y el catálogo se carga
de forma **perezosa** (en la primera llamada, con caché por proceso). Esto
significa que un catálogo empaquetado roto — JSON malformado, un slug
duplicado, una colisión — **no** hace fallar la importación del DAG. Se
degrada silenciosamente: toda llamada devuelve `None`, todo consumidor recae
en su comportamiento previo de nombre completo, y el fallo se registra como
un único `ERROR` la primera vez que ocurre.

Debido a ese fallback silencioso, **el test de CI del catálogo empaquetado
(`tests/congress_videos/test_politician_display_names.py`, el caso que carga
el `politician_display_names.v1.json` real) es la verdadera puerta de
control.** Nunca debe omitirse ni debilitarse — es lo único que convierte un
catálogo roto en un fallo de CI ruidoso en lugar de una regresión silenciosa
e inadvertida en los títulos en producción.

## Qué se deja deliberadamente sin canonicalizar

- **Las personas mencionadas siempre renderizan su nombre completo.** El
  catálogo solo se consulta para el sujeto resuelto de un título, una
  miniatura o un short — nunca para una persona simplemente referenciada al
  hablar.
- **Un apellido a secas solo es seguro para el sujeto del vídeo.** Renderizar
  un apellido a secas para alguien simplemente mencionado sería mucho más
  propenso a resultar ambiguo o engañoso que para el orador identificado
  sobre el que trata realmente el contenido, por lo que el cableado nunca
  enruta el slug de una persona mencionada a través de
  `canonical_display_name`.
