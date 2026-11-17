# Third-party source data

Publisher: New York City Taxi & Limousine Commission. Trip records were supplied to TLC by authorized technology providers. TLC does not warrant the accuracy of the records.

- Source and data dictionaries: https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
- NYC terms: https://www.nyc.gov/home/terms-of-use.page
- Taxi zone lookup: https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv

`seed/yellow_2025-01_sample.parquet` is a convenience subset of the official January 2025 Yellow Taxi file, with original columns retained. `seed/provenance.json` describes its deterministic selection, source URL and SHA-256 checksums. `zone_lookup.csv` is the official reference table. Data is included for reproducible study; the application MIT licence does not relicense third-party data or waive publisher terms. Check the publisher's terms for your intended use. No TLC endorsement is implied.
