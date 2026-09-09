# Manual Upload Quota Specification

## Requirement: scheduled quota has durable attribution

Successful upload records SHALL persist whether they count toward the daily scheduled quota. Scheduled DAG runs persist `TRUE`; manual and forced runs persist `FALSE`.

#### Scenario: manual recovery then scheduled run
- **GIVEN** a manual run uploads a turn today
- **WHEN** the scheduled run checks the quota later today
- **THEN** the manual upload is excluded and the scheduled run can proceed.

#### Scenario: two scheduled runs
- **GIVEN** a scheduled run uploads today
- **WHEN** another scheduled run checks the quota
- **THEN** it remains blocked by the daily limit.
