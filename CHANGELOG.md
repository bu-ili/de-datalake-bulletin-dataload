# Changelog


## [v1.1.5] - 2026-01-28
- Redid the asset logic to create two separate assets, instead of dynamic multi asset configuration that was an overengineered solution
- Added a hash function to encrypt the data into additional "dl_hash" column
- Added a sensor that interacts with Bulletin Wordpress API, and only runs if new data is available per latest Dagster DAG run variable compared to latest modified date of each API endpoint

## [v1.1.4] - 2026-01-27
- Changed static single asset to dynamically create multiple assets driven by a JSON config file
- Formated the response from the API endpoint to conform to the DE standard ("id", "dl_insterted_at", "payload")
- Abstracted path parameters into configurable variables instead of hard coded values

## [v1.1.3] - 2026-01-23
- Removed database connections and dependencies
- Created an upload function to create appopriate folder structure and push parquet file data export to AWS S3
- Separated functions into digestable Python files that are subsequently used in asset generation and execution 

## [v1.1.2] - 2026-01-21
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

