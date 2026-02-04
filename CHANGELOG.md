# Changelog

## [v1.3.0] - 2026-02-04
### CI/CD & Deployment
- Implemented GitHub Actions workflow with CalVer tagging strategy
  - Dev branch pushes to `ghcr.io/bu-ili/de-bulletin:dev`
  - Test branch pushes to `ghcr.io/bu-ili/de-bulletin:test`
  - Main branch pushes to `ghcr.io/bu-ili/de-bulletin:latest` and `YYYYMMDD-{sha}` tags
- Migrated from local k3d-only workflow to dual deployment strategy (GHCR + local k3d)

### Kubernetes
- Updated deployment manifests to use GitHub Container Registry (ghcr.io)
- Added `imagePullSecrets` configuration for GHCR authentication
- Enhanced security context with explicit `runAsNonRoot: true` at container level
- Updated image references from placeholder ACR to production GHCR paths

### Configuration
- Standardized ConfigMap naming conventions (removed typo in USER_AGENT)
- Updated Secret templates with clearer placeholder documentation
- Enhanced k8s/README.md with comprehensive deployment instructions
  - Added GHCR authentication setup
  - Documented CalVer tagging strategy and usage patterns
  - Added troubleshooting section for common deployment issues
  - Included rollback procedures and image tag management

### Developer Experience
- Created sync tooling for local k3d testing environment
  - `sync-from-ghcr.sh` - Pull images from GHCR to local k3d registry
  - `bulletin.conf` - Centralized configuration for sync scripts
- Simplified local development workflow with clear separation between fast iteration (local builds) and production-like testing (GHCR images)

## [v1.2.0] - 2026-02-02
### Performance & Optimization
- Optimized async HTTP fetching with HTTP/2 support (50 max connections, 20 keepalive connections)
- Improved retry logic with exponential backoff (5s-60s, up to 5 attempts) using tenacity
- Simplified S3 upload logic for small parquet files (removed multipart upload)

### Configuration & Architecture
- Migrated `base_url` from environment variable to `config.json` for centralized configuration
- Added validation to `ConfigResource` (base_url, endpoints, pagination fields)
- Added validation to `AWSS3Resource` (lazy client initialization with bucket existence check)
- Fixed `AWSS3Resource` error handling (removed invalid boto3.client type annotation)

### Code Quality
- Standardized all docstrings to Google Style format across codebase
- Removed unused imports (dagster as dg, contextlib, datetime from specific files)
- Updated all function/class docstrings with proper Args, Returns, Raises sections

### Docker & Deployment
- Optimized Dockerfile by removing redundant uv bootstrap in runtime stage
- Fixed permission issues by creating `/opt/dagster` and `/bulletin_raw` directories with proper ownership
- Added `dagster-postgres` dependency for Kubernetes gRPC deployment (instance storage)
- Set `PYTHONPATH=/app/src` for proper module imports

### Bug Fixes
- Fixed sensor to work properly with Kubernetes deployment (uses `context.resources.get_config`)
- Fixed environment variable quote handling in `.env` file
- Updated Kubernetes configmap and deployment manifests for new configuration structure

## [v1.1.5] - 2026-01-28
### Architecture Changes
- Refactored asset logic to create two separate named assets (`bulletin_pages`, `bulletin_media`) instead of dynamic multi-asset configuration
- Simplified asset structure for better maintainability and debugging

### Data Quality
- Added hash function to generate `dl_hash` column for data integrity verification
- Hash computed from concatenation of all column values with hex encoding

### Automation
- Implemented `bulletin_data_sensor` for automated content change detection
- Sensor compares latest modified dates from WordPress API against cursor-stored values
- Only triggers asset materialization when new or modified content is detected
- Uses cursor persistence to track state between sensor evaluations

## [v1.1.4] - 2026-01-27
### Configuration Management
- Introduced `config.json` for endpoint configuration (dynamic endpoint definition)
- Abstracted pagination and sensor parameters into configuration file
- Enabled adding new endpoints without code changes

### Data Standards
- Formatted API responses to conform to DE standard schema: `id`, `dl_inserted_at`, `payload`
- Standardized timestamp field as `dl_inserted_at` for data lineage tracking

### Code Organization
- Created `ConfigResource` for centralized configuration loading
- Removed hard-coded endpoint paths and URL parameters

## [v1.1.3] - 2026-01-23
### Architecture Simplification
- Removed database connections and dependencies (DuckDB removal)
- Eliminated intermediate database storage layer
- Direct pipeline: API → Validation → Parquet → S3

### AWS Integration
- Implemented S3 upload functionality with `AWSS3Resource`
- Created folder structure preservation for data lake organization
- Path format: `/bulletin_raw/{endpoint}/load_date={date}/load_time={time}/`

### Code Structure
- Separated concerns into modular files: `resources.py`, `validators.py`, `data_exporters.py`, `assets.py`
- Improved code maintainability and testability
- Created reusable export functions

## [v1.1.2] - 2026-01-21
### Data Pipeline Changes
- Migrated from DuckDB to PostgreSQL for DE standardization (later removed in v1.1.3)
- Switched Parquet export to Polars library (replacing DuckDB's export functionality)
- Improved Parquet write performance with Polars DataFrame operations

### Documentation
- Updated documentation to align with DE data engineering standards
- Added comprehensive docstrings for functions and classes

## [v1.1.1] - 2026-01-15
### Repository Migration
- Renamed repository from `de-bulletin-ingestion` to `de-datalake-bulletin-dataload`
- Aligned naming convention with DE data engineering standards

### HTTP Client Upgrade
- Replaced `aiohttp` with `httpx` for async HTTP requests
- Improved HTTP/2 support and connection management
- Better exception handling and retry capabilities

### Dagster Integration
- Added Dagster resources (`HTTPClientResource`, `ParquetExportResource`)
- Created `definitions.py` for Dagster Web UI compatibility
- Enabled asset materialization via Dagster interface

## [v1.1.0] - 2026-01-06
### Initial Release
- Created initial pipeline in `de-bulletin-ingestion` repository
- Implemented async data collection from BU Bulletin WordPress API
- Added DuckDB local storage for fetched data
- Created basic documentation and README
- Established project structure for data ingestion workflow

