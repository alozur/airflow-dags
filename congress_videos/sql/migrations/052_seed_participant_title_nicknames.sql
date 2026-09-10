-- Migration 052: seed congress_participants.nickname with curated title names (issue #510).
--
-- `nickname` answers one question: how should this person be named in a YouTube
-- title? A curated value means the audience recognises them and the name earns
-- its place; an empty value means they do not, and the title should reach for
-- the office or the party instead. The 60-title labelling pass behind
-- benchmarks/title_eval found unrecognised surnames to be dead weight — "no le
-- conoce la gente pero un ministro da notoriedad".
--
-- Curating this here rather than listing names in the prompt is deliberate.
-- Issue #91 removed politician names from THUMBNAIL_TITLE_SYSTEM_PROMPT because
-- their presence made the generator attribute quotes to people who never spoke,
-- and that guard is pinned by a test. Names belong in data, keyed to the
-- speaker actually resolved for the turn.
--
-- Names chosen by the maintainer. Extend or edit this column directly; no code
-- change is needed for a new entry to take effect.
--
-- Guarded by `WHERE nickname IS NULL OR nickname = ''` so a value edited by
-- hand after this migration ran is never clobbered by a re-run.

UPDATE congress_participants AS cp
SET nickname = v.nickname,
    updated_at = NOW()
FROM (VALUES
    ('pedro-sanchez-perez-castejon',             'Pedro Sánchez'),
    ('gabriel-rufian-romero',                    'Rufián'),
    ('miriam-nogueras-i-camero',                 'Míriam Nogueras'),
    ('mertxe-aizpurua-arzallus',                 'Mertxe Aizpurua'),
    ('patxi-lopez-alvarez',                      'Patxi López'),
    ('miguel-tellado-filgueira',                 'Tellado'),
    ('jose-maria-figaredo-alvarez-sala',         'Figaredo'),
    ('oscar-puente-santiago',                    'Óscar Puente'),
    ('francina-armengol-socias',                 'Armengol'),
    ('maria-jose-rodriguez-de-millan-parro',     'Pepa Millán'),
    ('veronica-martinez-barbero',                'Verónica Martínez'),
    ('nestor-rego-candamil',                     'Néstor Rego'),
    ('alberto-nunez-feijoo',                     'Feijóo'),
    ('santiago-abascal-conde',                   'Abascal'),
    ('cayetana-alvarez-de-toledo-peralta-ramos', 'Álvarez de Toledo'),
    ('ione-belarra-urteaga',                     'Belarra'),
    ('yolanda-diaz-perez',                       'Yolanda Díaz'),
    ('concepcion-gamarra-ruiz-clavijo',          'Gamarra'),
    ('ester-munoz-de-la-iglesia',                'Ester Muñoz')
) AS v(slug, nickname)
WHERE cp.slug = v.slug
  AND (cp.nickname IS NULL OR cp.nickname = '');

-- DOWN (manual only; the migration runner executes the whole file in one
-- transaction, so an uncommented DOWN block would silently revert the UP):
-- UPDATE congress_participants SET nickname = NULL WHERE slug IN (
--     'pedro-sanchez-perez-castejon', 'gabriel-rufian-romero',
--     'miriam-nogueras-i-camero', 'mertxe-aizpurua-arzallus',
--     'patxi-lopez-alvarez', 'miguel-tellado-filgueira',
--     'jose-maria-figaredo-alvarez-sala', 'oscar-puente-santiago',
--     'francina-armengol-socias', 'maria-jose-rodriguez-de-millan-parro',
--     'veronica-martinez-barbero', 'nestor-rego-candamil',
--     'alberto-nunez-feijoo', 'santiago-abascal-conde',
--     'cayetana-alvarez-de-toledo-peralta-ramos', 'ione-belarra-urteaga',
--     'yolanda-diaz-perez', 'concepcion-gamarra-ruiz-clavijo',
--     'ester-munoz-de-la-iglesia'
-- );
