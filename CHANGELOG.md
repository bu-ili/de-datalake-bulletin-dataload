# Changelog

## [v1.1.2] - 2026-01-20
- Changed underlying database from DuckDB to Postgres to conform to DE standard
- Changed Parquet export function to using Polars instead of DuckDB components
- Improved documentation to conform to DE standard

## [v1.1.1] - 2026-01-15
- Migrated repo to de-datalake-bulletin-dataload to conform with DE naming convention
- Changed assets to use httpx instead of aiohttp 
- Added appropriate resources and definitions to enable ETL run in Dagster Web UI

## [v1.1.0] - 2026-01-06
- Initial commit in de-bulletin-ingestion repo
- Added assets to asynchronously collect and store BU Bulletin Wordpress API data in local instance of DuckDB
- Added documentation

