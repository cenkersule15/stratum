# STRATUM

**Data reliability, from source to published tables.** A local data engineering project by Cenker Sule.

[Türkçe](README.tr.md) · [Deutsch](README.de.md)

Python · DuckDB · dbt Core · Dagster · Parquet · FastAPI · React · TypeScript · Docker Compose

## Run in VS Code

Extract the ZIP and open the **stratum** folder containing `compose.yaml` in VS Code. Start Docker Desktop, then use its terminal:

```bash
docker compose up --build
```

Open **http://localhost:8080**. API documentation: **http://localhost:8080/api/docs**.

The first build downloads dependencies. The first API startup processes the bundled real 50,000-row sample; allow it to finish before launching another run. No API key, cloud account or host Python/Node installation is required. Processing results persist in the `stratum-data` Docker volume. `docker compose down` stops the app without deleting that volume. Do not use `down -v` unless you intend to erase your local run history and datasets. If port 8080 is occupied by AFTERLIGHT, stop it or change the host port in Compose to 8081.

Recommended development budget: 8 GB host RAM, Docker allocated at least 3 GB, and 3 GB free disk for full-month experiments. The small sample is the default. Exact time and memory depend on hardware; this is not a production capacity guarantee.

## What is implemented

- Real NYC TLC Yellow Taxi Parquet data; a bundled convenience sample works offline after dependencies are installed.
- January-March 2025 full-month ingestion through a fixed allowlist, cached downloads, three attempts and recorded SHA-256. January source and the bundled sample/reference table are pinned by checksum.
- A persisted SQLite control ledger with run status, input mode, partition, source digest, policy version, per-stage events and timings.
- Required-column checks, numeric/timestamp conversion and six row-level business rules. Unexpected additional columns are allowed and preserved in the raw source; required missing columns block publication.
- Separate accepted and quarantined Parquet files, reason groups and a 50-row quarantine preview. Multiple rule failures may overlap; reason counts are not additive.
- Hash/policy-based no-op detection. Reprocessing the same published source and policy does not append records. Full and sample inputs for the same month **replace** one another as a whole partition.
- dbt SQL staging view, daily and zone aggregate tables, ten data tests including row-count and decimal-amount reconciliation.
- A candidate warehouse per execution. Only after dbt succeeds does one SQLite transaction update both the active-partition manifest and the active-warehouse pointer. Failed runs cannot replace the last good data.
- Dagster materializes one `warehouse_partition` asset per requested execution. Detailed substeps live in the control ledger; SQL dependencies are in the dbt manifest. No claim of separate Dagster assets for every visual node.
- Browser console in English, Turkish and German, dark/light themes, history filters, source hashes, data explorer, code-defined lineage, isolated failure drills and JSON/log downloads.

## Demonstrate engineering behaviour

1. Let the automatic sample run finish. January sample: 50,000 raw rows, 46,797 accepted, 3,203 quarantined under `yellow-v1.0`.
2. Click **Run pipeline** with the same sample and standard scenario. Status becomes **Unchanged**, with no duplicate publication.
3. Select **Missing-column drill**. It removes `total_amount` from an isolated validation copy. The contract blocks the run and the published record count remains unchanged.
4. Select **Invalid-distance drill**. It changes distances to -999 in a copy. Row acceptance falls below 80%, so publication is blocked. Inspect the quarantine.
5. Run the clean source again. If the same source is already published this is an idempotent no-op, not a fabricated recovery rebuild.
6. Choose **Full month** for January, February or March to download and process the original file. Each monthly partition can be backfilled individually. There is no automatic multi-month scheduler or streaming system in this release.

## Data semantics and limits

Official source: [NYC TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page). Attribution, checksums, selection and original file size/rows are recorded in `data/seed/provenance.json`. The January source contains 3,475,226 rows. The sample selects every 69th source row up to 50,000 records; it is not a random or representative sample.

Policy `yellow-v1.0` accepts trips within the requested pickup month, with positive duration up to 360 minutes, finite distance in (0,200] miles, lookup-valid pickup/dropoff zones, nonnegative fare/total, total at most USD 1,000 and payment codes 0-6. These are project rules, not TLC error definitions: valid refunds, long trips and corrections may be quarantined. Dates remain provider-local naive timestamps; no timezone conversion is claimed. `total_amount` aggregates are not audited revenue.

Exact-source-row MD5 fingerprints are conservative duplicate **candidates**, not publisher-issued trip IDs. Legitimate identical trips can be removed by this policy. All originals remain available in raw files. SHA-256 is used for file integrity, not MD5. The system does not merge row-level corrections across monthly files, perform CDC or guarantee exactly-once delivery. Backfills rebuild the aggregate warehouse from the current partition manifest. Raw and historical snapshot files accumulate; there is no retention cleanup job.

The API is an intentionally local, single-worker application. It serializes submissions and pipeline writes in one process; it is not a distributed queue. Interrupted jobs are marked on restart, not automatically resumed mid-stage. Completed downloads are cached; failed downloads retry from the beginning. No authentication, tenant separation or production SLA is provided. Do not expose the app publicly.

## Native development

Python 3.12 and Node 24:

```bash
python3.12 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
npm --prefix frontend ci
npm run dev
```

Native console: **http://localhost:4173**. The backend uses port 8091 and `PYTHONPATH=backend`. `scripts/dev.mjs` starts both processes. Dagster configuration disables telemetry in the local data directory. dbt anonymous usage collection is disabled.

## Verification

```bash
PYTHONPATH=backend .venv/bin/python -m unittest discover -s backend/tests -v
npm --prefix frontend run build
```

See `docs/VALIDATION.md`, `docs/ARCHITECTURE.md` and `docs/INTERVIEW.md`. Tests exercise publication, dbt failure, duplicate candidates, reconciliation, idempotency and fail-closed drills. Native checks do not substitute for Docker validation: Docker is unavailable in the authoring environment, so container builds must be verified on your machine or CI.

## Licence

Original application code: MIT, see `LICENSE`. NYC source records are third-party material and are **not relicensed as MIT**. See `data/LICENSE.md` and the publisher's terms. External UI fonts use Google Fonts with system fallbacks; the app remains usable offline with fallback fonts.
