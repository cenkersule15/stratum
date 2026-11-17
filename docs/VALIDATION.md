# Validation record

Date: 16 November 2026. Native Python 3.12 / Node 24, Linux AMD64 authoring environment.

## Pipeline checks

Four unittest methods pass. They cover successful sample publication and 13 dbt result entries (3 models, 10 data tests), raw/accepted/quarantine reconciliation, same-input idempotent skips, missing-column and invalid-distance blocked runs, preservation of the last good warehouse, injected dbt failure without publication, exact-row duplicate candidates, and invalid partition/scenario rejection.

Native FastAPI TestClient checks passed for health, overview, four table endpoints, run detail, log, JSON attachment, unknown-run 404 and unsupported-partition 422. Source data is real; failure drills deliberately modify isolated validation copies.

## Real sample

50,000 raw rows; 46,797 accepted; 3,203 quarantined. Acceptance: 93.594%. These are business-policy decisions, not measured source-data accuracy. Counts are disjoint for accepted/quarantined. Individual rule-failure counts can overlap.

## Full-month processing

Official January 2025 Yellow Taxi file, pinned by SHA-256 in provenance:

| Measure | Observed value |
|---|---:|
| Raw rows | 3,475,226 |
| Accepted rows | 3,251,812 |
| Quarantined rows | 223,414 |
| Engine processing, including dbt | 45.56 seconds |
| dbt models + tests | 13 successful results |

The final engine uses a 1 GB DuckDB working-memory setting, one processing thread and insertion-order preservation disabled. Timing is one native authoring run on a cached original file; it is not a hardware-independent benchmark, download-time estimate, peak-memory measurement or production SLA. A 512 MB first attempt exhausted DuckDB memory and did not publish; the working-memory configuration was adjusted before the successful full-month run. See `full-month-validation.json` for recorded source hash, rule counts, events and dbt results. February and March are supported inputs but were not downloaded/processed during this validation.

## Frontend

TypeScript and Vite production build passed. Browser checks cover actual source-backed overview values, run creation, blocked missing-column and invalid-distance scenarios, idempotent skip, execution details, quarantine preview and reasons, dbt results, explorer/schema tabs, lineage navigation, TR/EN/DE and light/dark mode. Responsive styles are included; device-specific mobile browser testing was not performed.

Production npm audit: zero reported vulnerabilities at validation time. This is a frontend-production dependency audit, not a complete application security certification or Python dependency audit.

## Verification boundary

Docker is not installed in the authoring environment. Dockerfiles/Compose are supplied, but container builds and `docker compose up --build` were not executed here. Final container validation must run on the target Docker host. Local-only single-user scope; authentication, distributed jobs and production recovery semantics are outside this release.
