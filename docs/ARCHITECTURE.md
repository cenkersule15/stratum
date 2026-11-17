# Architecture and operating model

## Persistent state

`var/` in native development, `/data` in Docker:

| Path | Purpose |
|---|---|
| bronze/ | Completed full-month source downloads; raw records unchanged |
| runs/<id>/ | Validation DuckDB, dbt manifest/run-results and console log |
| silver/<id>.parquet | Immutable accepted candidate partition |
| quarantine/<id>.parquet | Rejected/duplicate-candidate rows and reasons |
| warehouse/<id>.duckdb | Candidate then published analytics snapshot |
| control.sqlite | Run ledger, active partitions, active warehouse pointer |

The bundled sample is an immutable input under `data/seed/`, not copied into bronze. Browser refreshes query the API; no displayed metric is generated randomly. The only fabricated inputs are explicit isolated failure drills.

## Publication protocol

1. Acquire the single-process pipeline lock. Mark the run running.
2. Resolve an allowlisted source and check available pinned hashes. For February/March, SHA-256 records observed bytes; it is not an independently trusted pre-download signature.
3. If hash and policy match the currently published partition, record an idempotent skip. No transformation/publication is performed.
4. Validate required fields, cast types, assign rule reasons and classify exact-row duplicate candidates.
5. Write quarantine and accepted candidate files. Require nonempty input and at least 80% acceptance.
6. Rebuild a candidate warehouse using all currently active monthly partitions, replacing the requested month's input with its candidate.
7. Run dbt build. Ten tests plus three models must complete successfully.
8. Update the active partition manifest and active warehouse path in one SQLite transaction.

API readers resolve an immutable warehouse filename through the control database. In-progress transformation does not modify their current file. Files left behind by failed runs are unreferenced audit artifacts. A crash before pointer update leaves the prior warehouse active. Startup marks interrupted queued/running jobs; it does not resume the middle of a job. The run success marker follows the publication transaction, so a crash between those writes can leave an active partition associated with an interrupted run; the manifest is authoritative.

## Orchestration and lineage

Dagster executes one software-defined asset, `warehouse_partition`, configured with the existing run ID. Its internal stages are tracked in the SQLite ledger. The UI's stage/asset map is code-defined, not a claim that every box is a separate Dagster node. dbt provides `manifest.json` and `run_results.json` for the staging and aggregate graph. The UI lists those actual results per run.

## Deliberate scope

One API worker, in-process background thread, one writer lock, SQLite metadata and local Parquet/DuckDB. There is no distributed work queue, row-level CDC, live stream, auth or tenant model. Container restarts preserve the volume. New full partitions rebuild the warehouse rather than incrementally merging corrected trips. No arbitrary SQL or arbitrary remote URL is accepted by the API. History views show the latest 40 runs; files and control records remain on disk until explicitly managed outside the app.

DuckDB pipeline working-memory limit: 1 GB, one processing thread, insertion-order preservation disabled. dbt uses two threads and a 512 MB DuckDB limit. These are engine settings, not measured peak-process-memory guarantees. No synthetic query speed-up benchmark is presented.

`source_row` is a run-local scan index, not a guaranteed original Parquet file offset. Record fingerprints and partition/source hashes support reproducibility; row-level lineage is not presented as a publisher-certified trip identity.
