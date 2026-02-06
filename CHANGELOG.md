# Changelog

## [v1.6.0] - 2026-02-06
### Kubernetes & Containerization Optimization
- **Removed local file cleanup logic** for containerized environments
  - Deleted `cleanup_old_local_files()` function and all references
  - emptyDir volumes in Kubernetes automatically handle cleanup on pod restart
  - Simplified codebase by removing unnecessary file retention logic

- **Migrated from cursor-based to S3-based baseline detection**
  - Schedule no longer uses cursor for state management
  - Assets auto-discover baseline by querying S3 `LastModified` metadata
  - Created `get_last_modified_from_s3()` function using `list_objects_v2()`
  - Extracts date from S3 partition keys with regex: `load_date=(\d{4}-\d{2}-\d{2})`
  - Defaults to `2000-01-01T00:00:00` when no S3 objects found (first run)
  - Removed dependencies on local Parquet files for baseline detection

- **Simplified schedule configuration**
  - Removed all cursor logic from `bulletin_daily_schedule`
  - Schedule now just triggers assets every 5 minutes (testing mode)
  - Assets handle baseline discovery independently
  - Hardcoded `upload_to_s3=False` and `full_refresh=False` for testing

### User Experience Enhancements
- **Added field descriptions to RuntimeConfig** for Dagster UI
  - All config fields now display helpful descriptions in Launchpad
  - Users see guidance on format and purpose when materializing assets
  - Migrated from `dagster.Field` to `pydantic.Field` for proper Pydantic Config support

- **Made last_modified_date user-friendly**
  - Accepts simple `YYYY-MM-DD` format instead of full ISO 8601
  - Automatically transforms to `YYYY-MM-DDTHH:MM:SS` for API calls
  - Updated docstrings to reflect new user-friendly format
  - Applies to both user-provided and S3-discovered dates

### Code Quality & Logging
- **Standardized logging across utilities**
  - Made `context` a required parameter in `get_last_modified_from_s3()`
  - Removed all `print()` statements in favor of `context.log`
  - Ensures all logs appear in Dagster UI with proper context

- **Improved file size reporting**
  - Changed S3 upload logs from KB to MB format
  - Display format: `{size:.2f} MB` instead of `{size:.1f} KB`

### Bug Fixes
- **Fixed S3-discovered date format inconsistency**
  - S3-extracted dates now properly transformed to ISO 8601 format
  - Ensures consistent `YYYY-MM-DDTHH:MM:SS` format across all baseline sources
  - Prevents potential API filter issues with partial date formats

### Documentation
- **Created comprehensive behavior matrix** (`BEHAVIOR_MATRIX.md`)
  - Complete matrix of trigger types and runtime config combinations
  - Expected behavior for each configuration scenario
  - Decision logic flow diagrams
  - Use case examples for testing, production, and manual operations
  - Added to `.gitignore` and `.dockerignore` as internal reference

- **Enhanced README.md**
  - Added "Runtime Behavior" section with quick configuration examples
  - Documented all runtime config parameters with descriptions
  - Included configuration precedence hierarchy
  - Added link to `BEHAVIOR_MATRIX.md` for detailed reference

## [v1.5.0] - 2026-02-05
### Code Refactoring & Architecture
- **Centralized all configuration constants** in `config.json`
  - Moved all hardcoded path variables to config (partition prefixes, file extensions, etc.)
  - Eliminated explicit string concatenations that produced URLs throughout codebase
  - Enhanced maintainability and consistency using config.json

- **Consolidated ConfigResource methods** for simplified API
  - Removed individual getter methods (`get_base_url()`, `get_pagination_param()`, etc.)
  - Introduced unified `get_config_value(key, default, required)` method
  - Consistent error handling with clear validation messages

- **Resolved circular import** between `schedules.py` and `assets.py`
  - Created `utilities.py` module for shared functions
  - Moved `get_last_modified_from_parquet()` and `fetch_latest_modified_date()` to utilities
  - Improved code organization and dependency structure

- **Migrated cleanup functionality** to utilities module
  - Moved `cleanup_old_local_files()` from `data_exporters.py` to `utilities.py`
  - Decoupled cleanup logic from S3 export function
  - Assets now call cleanup directly after successful S3 upload
  - Made `get_config` a required parameter

### Bug Fixes
- **Fixed URL construction** for paginated API requests
  - Corrected page parameter format to `&page=` instead of improper concatenation
  - Ensures proper API query string formatting

### Code Quality
- Used config-driven approach for all path construction
- Improved error messages and validation in ConfigResource
- Enhanced code modularity

## [v1.4.0] - 2026-02-05
### Architecture & Performance
- **Migrated from sensor to schedule-based execution** for cost reduction in production
  - Changed from 24/7 running sensor to daily cron schedule (10:00 AM UTC)
  - Reduces EKS pod runtime from 730 hours/month to ~10 hours/month
  
- **Implemented idempotent incremental loads** using WordPress API `modified_after` filter
  - Assets now receive `last_modified_date` from schedule cursor
  - Only fetches records modified since last successful run
  - Supports `full_refresh=True` flag to bypass incremental logic
  - Gracefully handles empty responses (no new data since last run)

### Configuration
- Made HTTP/2 support runtime configurable via `config.json`
  - Added `http2` boolean flag to `http_client` configuration block
  - Defaults to `true` for better performance and reduced server load
  - Can be disabled for compatibility or debugging purposes
- Enhanced `RuntimeConfig` with incremental load parameters:
  - `last_modified_date`: ISO 8601 datetime for filtering API queries
  - `full_refresh`: Boolean flag to force complete data refresh

### Code Quality
- Updated `fetch_all_pages()` to support filtered API queries with `modified_after` parameter
- Enhanced asset functions to handle empty data responses gracefully
- Converted `bulletin_data_sensor` to `bulletin_daily_schedule` with cursor support
- Updated docstrings to reflect schedule-based execution model

### Migration Notes
- Schedule runs daily at 10:00 AM UTC (configurable via `cron_schedule`)
- Cursor format unchanged: `pages_modified|media_modified`
- For manual runs, set `full_refresh=true` to fetch all data

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

