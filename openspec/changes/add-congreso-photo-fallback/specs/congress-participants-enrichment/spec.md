# Delta for congress-participants-enrichment

## ADDED Requirements

### Requirement: Congreso.es Fallback Task Exists as Separate DAG Step

The DAG `congress_participants_sync` MUST include a fourth task `t4`
(`fill_congreso_photo_fallback`) wired strictly downstream of `t3`
(`enrich_missing_photos`). `t4` MUST NOT run unless `t3` has completed
successfully.

#### Scenario: t4 is present and correctly wired

- GIVEN the DAG `congress_participants_sync` is loaded
- WHEN task dependencies are inspected
- THEN the DAG contains exactly 4 tasks
- AND `t4` has `t3` as its sole upstream dependency

---

### Requirement: Bulk SearchDiputados Fetch Per Run

The fallback MUST issue exactly one bulk POST to `CONGRESO_SEARCH_DIPUTADOS_URL`
per DAG run using a browser User-Agent header to retrieve all active deputies
and their `codParlamentario` values. It MUST NOT issue one request per deputy.

#### Scenario: Successful bulk fetch returns deputy list

- GIVEN the searchDiputados endpoint is reachable
- WHEN `fill_congreso_photo_fallback` executes
- THEN exactly one POST request is made to `CONGRESO_SEARCH_DIPUTADOS_URL`
- AND the result is a mapping of `normalized_name → codParlamentario` covering
  all deputies in the response

#### Scenario: Browser User-Agent is sent

- GIVEN the searchDiputados endpoint is reachable
- WHEN the POST is issued
- THEN the request includes a non-empty `User-Agent` header consistent with a
  desktop browser

---

### Requirement: Name Join Uses normalize_member_name on Both Sides

The fallback MUST join searchDiputados records to stored participants using
`normalize_member_name` applied to both the response name and the stored name.
Raw name strings MUST NOT be compared directly.

#### Scenario: Normalized names match despite formatting differences

- GIVEN a searchDiputados record with name `"García López, María"`
- AND a stored participant with normalized name matching that form
- WHEN the join is performed
- THEN the record is matched and `codParlamentario` is resolved

#### Scenario: No match when normalized names differ

- GIVEN a searchDiputados record whose normalized name does not match any
  stored participant
- WHEN the join is performed
- THEN no `photo_url` update is attempted for that record
- AND the miss is logged at WARNING level

---

### Requirement: Deterministic Photo URL Construction

For each matched deputy, the fallback MUST construct the photo URL as
`CONGRESO_PHOTO_URL_TEMPLATE` resolved with `codParlamentario` and
`LEGISLATURE_ID` (= 15). No other URL form is permitted.

#### Scenario: Photo URL constructed from codParlamentario and legislature

- GIVEN a matched deputy with `codParlamentario = "ABC123"`
- AND `LEGISLATURE_ID = 15`
- WHEN the photo URL is built
- THEN the result equals
  `https://www.congreso.es/docu/imgweb/diputados/ABC123_15.jpg`

---

### Requirement: Null Guard — Wikidata Photos Are Never Overwritten

The fallback MUST write `photo_url` only for participants whose `photo_url` is
currently `NULL`. Rows with an existing value (Commons or any prior URL) MUST
NOT be updated. The existing `update_photo_url` SQL guard
(`WHERE photo_url IS NULL`) satisfies this requirement.

#### Scenario: Deputy already has a Wikidata photo

- GIVEN a participant with an existing non-null `photo_url`
- WHEN `fill_congreso_photo_fallback` runs
- THEN that participant's `photo_url` is not changed

#### Scenario: Deputy has no photo after Wikidata enrichment

- GIVEN a participant with `photo_url = NULL` after `t3` completes
- AND a matching `codParlamentario` is found
- WHEN `fill_congreso_photo_fallback` runs
- THEN `photo_url` is set to the deterministic congreso.es URL

---

### Requirement: Graceful Degradation on Every Failure Mode

The fallback MUST log and skip on all of the following without failing the DAG
run: searchDiputados 4xx/5xx response, network error, name-join miss,
individual photo HTTP 404. It MUST NOT raise an uncaught exception that marks
the task failed.

#### Scenario: searchDiputados returns 403

- GIVEN the searchDiputados endpoint returns HTTP 403
- WHEN `fill_congreso_photo_fallback` executes
- THEN the error is logged at ERROR level
- AND the task completes with status SUCCESS (no photo updates performed)
- AND no exception propagates

#### Scenario: searchDiputados network error

- GIVEN the searchDiputados endpoint is unreachable (connection timeout)
- WHEN `fill_congreso_photo_fallback` executes
- THEN the error is logged at ERROR level
- AND the task completes with status SUCCESS

#### Scenario: Individual photo URL returns 404

- GIVEN a valid `codParlamentario` is resolved for a deputy
- AND the constructed photo URL returns HTTP 404
- WHEN `fill_congreso_photo_fallback` attempts to validate or write the URL
- THEN that deputy is skipped and logged at WARNING level
- AND other deputies in the batch are not affected

#### Scenario: Name-join miss for one deputy

- GIVEN one deputy in the stored list has no matching record in searchDiputados
- WHEN the join is performed
- THEN only that deputy is skipped
- AND remaining deputies are processed normally

---

### Requirement: New Constants Defined

The module MUST expose `CONGRESO_SEARCH_DIPUTADOS_URL` and
`CONGRESO_PHOTO_URL_TEMPLATE` as named constants in
`congress_videos/config/constants.py`. Magic string literals for the endpoint
or URL pattern MUST NOT appear elsewhere in the codebase.

#### Scenario: Constants are importable

- GIVEN the constants module is imported
- WHEN `CONGRESO_SEARCH_DIPUTADOS_URL` and `CONGRESO_PHOTO_URL_TEMPLATE` are
  accessed
- THEN both resolve to non-empty strings with the expected URL shapes

---

## MODIFIED Requirements

### Requirement: DAG Task Count

The DAG `congress_participants_sync` MUST contain exactly 4 tasks.
(Previously: exactly 3 tasks — `t1`, `t2`, `t3`.)

#### Scenario: DAG loads with four tasks

- GIVEN the DAG file is imported
- WHEN the task list is inspected
- THEN it contains exactly 4 tasks: `t1`, `t2`, `t3`, `t4`
- AND no import errors are raised
