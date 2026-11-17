# Explaining STRATUM in an interview

## Problem

An analytical dashboard is only as reliable as its inputs. A missing field, repeated import or bad monthly file can silently distort metrics. STRATUM makes validation, publication and rejected records inspectable.

## Demonstrate five decisions

1. **Idempotency:** the file digest plus versioned policy is compared with the active partition. A repeated input is recorded as skipped.
2. **Fail closed:** missing columns, low acceptance or a failed dbt build prevent publication. A controlled drill proves that the last good warehouse remains readable.
3. **Reconciliation:** every source row is either accepted or quarantined, including duplicate candidates. dbt checks that daily row counts and decimal amounts agree with staging.
4. **Atomic visibility:** immutable candidate snapshots are prepared first; a small transaction publishes their pointer and partition manifest together.
5. **Honest semantics:** rejected rows can be legitimate refunds or long trips. Exact-row fingerprints are not authoritative trip IDs. State those limits instead of claiming perfect data quality.

## Why these tools

DuckDB reads Parquet and makes local SQL processing practical. dbt separates SQL models/tests from orchestration. Dagster owns the partition execution entry point. SQLite keeps small operational metadata separate from analytical snapshots. React/FastAPI exposes operational evidence rather than hard-coded success counters. Docker Compose packages the local workflow.

## What to improve next

Durable external job queue, authenticated deployment, stronger external data contracts, object-store storage, full type-compatibility policy, retention cleanup, broader source coverage, corrected-record identity semantics and restart-safe checkpointing. Only add these when their need can be demonstrated.

## CV description

**EN:** Built a Docker-based data reliability workspace with real NYC TLC data, DuckDB/Parquet processing, dbt transformations and tests, Dagster execution, quarantine inspection and hash-based idempotent publication.

**TR:** Gerçek NYC TLC verisi, DuckDB/Parquet işleme, dbt dönüşüm ve testleri, Dagster çalıştırma, karantina inceleme ve içerik özetiyle güvenli tekrar çalıştırma sunan Docker tabanlı veri güvenilirliği platformu geliştirildi.

**DE:** Docker-basierten Arbeitsbereich für Datenzuverlässigkeit mit echten NYC-TLC-Daten, DuckDB/Parquet, dbt-Transformationen und Tests, Dagster-Ausführung, Quarantäneprüfung und hashbasierter idempotenter Publikation entwickelt.
