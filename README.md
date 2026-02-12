<h1 align="center">Bulletin Data DAG</h1>

<h2 align="center"><strong>An asynchronous DAG for loading BU Bulletin data</strong></h2>

<div align="center">

[![Version](https://img.shields.io/badge/Version-1.5.0-black.svg?logo=semanticrelease&logoColor=white)]()
[![Status](https://img.shields.io/badge/Status-Testing-orange.svg?logo=progress&logoColor=white)]()
[![Changelog](https://img.shields.io/badge/Changelog-View-blue.svg?logo=readthedocs&logoColor=white)](./CHANGELOG.md)

[![Python](https://img.shields.io/badge/Python-3.13.x-3776AB.svg?logo=python&logoColor=white)](https://www.python.org/downloads/)
[![Dagster Docs](https://img.shields.io/badge/Dagster-1.12.x-6352ff)](https://docs.dagster.io)
</div>

## Overview

This Dagster pipeline fetches data from the BU Bulletin WordPress API, validates responses, exports to Parquet format, and uploads to AWS S3. It includes a daily schedule that automatically detects new or modified content and triggers incremental data refreshes.

### Key Features
- **Idempotent incremental loads**: Only fetches records modified since last run using WordPress `modified_after` filter
- **Schedule-based execution**: Daily cron job (10:00 AM UTC) with 97% cost reduction vs 24/7 sensor
- **Multi-endpoint support**: Separate assets for pages and media endpoints
- **Optimized async HTTP**: Configurable HTTP/2 with 50 concurrent connections and exponential backoff (5-60s, 5 attempts)
- **Data validation**: Endpoint-specific Pydantic schemas with Google Style docstrings
- **Configurable exports**: Parquet files with hash integrity checks and Snappy compression
- **Optional S3 upload**: Runtime toggle for cloud storage integration
- **Full refresh support**: Bypass incremental logic to fetch all data when needed
- **Kubernetes-ready**: gRPC deployment with multi-stage Docker build using uv

## Dependencies
All Python runtime dependencies are defined in [`pyproject.toml`](pyproject.toml).

### Core Dependencies
| Library             | Version    | Notes                                        |
|---------------------|------------|----------------------------------------------|
| `dagster`           | `1.12.10`  | Core orchestration framework                 |
| `dagster-postgres`  | `>=0.28.10`| PostgreSQL storage backend (instance only)   |
| `httpx[http2]`      | `>=0.28.1` | Async HTTP client with HTTP/2 support        |
| `pydantic`          | `>=2.12.5` | Data validation and schema enforcement       |
| `polars`            | `>=1.37.1` | Fast DataFrame library for Parquet exports   |
| `tenacity`          | `>=9.1.2`  | Retry logic with exponential backoff         |
| `boto3`             | `>=1.42.33`| AWS SDK for S3 uploads                       |

### Development Dependencies
Development tools are defined in `[dependency-groups]`:

| Library              | Notes                                        |
|----------------------|----------------------------------------------|
| `dagster-webserver`  | Local Dagster UI for pipeline visualization  |

To install all dependencies including development tools:
```bash
uv sync --frozen
```

## Configuration

### Primary Configuration File (config.json)
All application configuration is centralized in [`config/config.json`](config/config.json). This includes API endpoints, paths, AWS settings, and database configuration.

#### Core Settings
```env
CONFIG_PATH="/app/config/config.json"  # Optional - defaults to /app/config/config.json
```

| Variable | Description | Required |
|----------|-------------|----------|
| `CONFIG_PATH` | Path to config.json file | No (defaults to /app/config/config.json) |

#### AWS Credentials (Environment Variables)
AWS credentials are kept as environment variables for security:
```env
AWS_ACCESS_KEY_ID="your-access-key"
AWS_SECRET_ACCESS_KEY="your-secret-key"
```

| Variable | Description | Required |
|----------|-------------|----------|
| `AWS_ACCESS_KEY_ID` | AWS access key | Only if `upload_to_s3=true` |
| `AWS_SECRET_ACCESS_KEY` | AWS secret key | Only if `upload_to_s3=true` |

**Note**: All other AWS settings (bucket name, region) are configured in config.json.

### API Configuration (config.json)
The [`config/config.json`](config/config.json) file defines all application behavior including WordPress API endpoints, file paths, AWS settings, and database configuration. This centralized approach eliminates hardcoded values and allows you to modify behavior without changing code.

**Example configuration:**
```json
{
    "base_url": "https://example.com/wp-json/wp/v2/",
    "endpoints": {
        "pages": "pages?",
        "media": "media?"
    },
    "pagination_param": "per_page=100",
    "loop_pagination_param": "&page=",
    "latest_modified_param": "?orderby=modified&order=desc&per_page=1",
    "idempotency_param": "&modified_after=",
    "default_baseline_datetime": "2000-01-01T00:00:00",
    "partition_date_prefix": "load_date=",
    "partition_time_prefix": "load_time=",
    "parquet_file_extension": "*.parquet",
    "http_client": {
        "max_connections": 50,
        "max_keepalive_connections": 20,
        "connect_timeout": 10.0,
        "total_timeout": 30.0,
        "http2": true
    },
    "paths": {
        "config_path": "/app/config/config.json",
        "parquet_export_folder_path": "/bulletin_raw/",
        "parquet_file_name": "de_bulletin_data.parquet",
        "parquet_compression": "SNAPPY"
    },
    "aws": {
        "s3_bucket_name": "your-bucket-name",
        "region_name": "us-east-1"
    },
    "postgres": {
        "host": "localhost",
        "port": 5432,
        "user": "de_bulletin_dataload",
        "password": "your-password",
        "database": "de_datalake_bulletin_data"
    }
}
```

| Field | Description | Usage |
|-------|-------------|-------|
| `base_url` | WordPress REST API base URL | Base URL for all API requests |
| `endpoints` | Dictionary of endpoint keys and their API paths | Each key creates a fetchable endpoint |
| `pagination_param` | URL parameters for initial request | Appended to endpoint URL (e.g., `per_page=100`) |
| `loop_pagination_param` | Parameter for paginated loop requests | Prepended to page number (e.g., `&page=`) |
| `latest_modified_param` | Query parameters for fetching latest modified date | Used by schedule to check for new content |
| `idempotency_param` | Parameter for incremental loads | Prepended to datetime filter (e.g., `&modified_after=`) |
| `default_baseline_datetime` | Fallback datetime for first run | Used when no previous data exists |
| `partition_date_prefix` | Prefix for date partition folders | Used in path: `{prefix}YYYY-MM-DD` |
| `partition_time_prefix` | Prefix for time partition folders | Used in path: `{prefix}HH:MM:SS` |
| `parquet_file_extension` | Glob pattern for Parquet files | Used to find existing files |
| `http_client` | HTTP client configuration settings | Controls connection pooling, timeouts, and HTTP protocol version |
| `paths` | Path configuration for exports and files | Parquet export paths and filenames |
| `aws` | AWS S3 configuration | Bucket name and region (credentials via env vars) |
| `postgres` | PostgreSQL database configuration | Connection details for database |

**HTTP Client Settings:**

| Setting | Default | Description |
|---------|---------|-------------|
| `max_connections` | 50 | Maximum concurrent connections to the API server |
| `max_keepalive_connections` | 20 | Maximum idle connections to keep alive for reuse |
| `connect_timeout` | 10.0 | Seconds to wait for connection establishment |
| `total_timeout` | 30.0 | Total seconds to wait for complete request |
| `http2` | true | Enable HTTP/2 protocol (recommended for better performance and reduced server load) |

**Note on HTTP/2**: When enabled, multiplexes multiple requests over fewer TCP connections, reducing overhead on both client and server. Falls back to HTTP/1.1 automatically if the server doesn't support HTTP/2. Set to `false` if you encounter compatibility issues.

**How it works:**
1. **ConfigResource** loads this file at runtime via `CONFIG_PATH`
2. **All modules** access config values through `ConfigResource.get_config_value(key, default, required)`
3. **HTTPClientResource** builds full URLs using config values
4. **Schedule** uses `latest_modified_param` to check for new content
5. **Assets** use `idempotency_param` for incremental loads
6. **Cleanup** uses partition prefixes to identify and remove old directories

**Configuration-driven architecture benefits:**
- No hardcoded strings in source code
- Consistent path construction across all modules
- Easy to modify behavior without code changes
- Centralized source of truth for all constants
- Better maintainability and testability

**Adding new endpoints:**
Simply add a new key-value pair to the `endpoints` object, create corresponding assets in [assets.py](src/de_datalake_bulletin_dataload/defs/assets.py), and add validation schemas in [validators.py](src/de_datalake_bulletin_dataload/defs/validators.py).

## Installation & Usage

### Installation
1. Clone the repository
2. Install dependencies using `uv`:
   ```bash
   uv sync --frozen
   ```
3. Configure your settings in [`config/config.json`](config/config.json)
4. (Optional) Create a `.env` file in the project root with AWS credentials if using S3 upload:
   ```env
   AWS_ACCESS_KEY_ID=your-access-key
   AWS_SECRET_ACCESS_KEY=your-secret-key
   ```

### Running the Pipeline

#### Start Dagster UI
```bash
uv run dagster dev
```
Open the Dagster UI at `http://127.0.0.1:3000`

#### Materialize Assets
- **Via Sensor**: Automatic execution when DAG is deployed in Dagster instance (tested via dg dev, and k3d/helm chart test environment)
- **Via UI**: Navigate to the Assets page and click "Materialize" on individual assets
- **Via CLI**: 
  ```bash
  dagster asset materialize -m de_datalake_bulletin_dataload.definitions --select bulletin_pages
  ```

#### Runtime Configuration
You can override settings when materializing assets:
```yaml
ops:
  bulletin_pages:
    config:
      upload_to_s3: true
      full_refresh: false  # Incremental load (default)
      last_modified_date: "2024-01-15"  # YYYY-MM-DD format, auto-converts to ISO 8601
```

## Runtime Behavior

The pipeline supports three execution modes with configurable baseline detection:

- **Scheduled incremental loads**: Auto-discovers baseline from S3 partitions (default)
- **Manual materialization**: Override config for testing, backfills, or full refreshes
- **First run (cold start)**: Defaults to historical baseline when S3 is empty

For the complete behavior matrix including all configuration combinations, see **[BEHAVIOR_MATRIX.md](BEHAVIOR_MATRIX.md)**.

### Quick Configuration Examples

**Standard Incremental Load (Default)**
```yaml
config:
  upload_to_s3: true
  full_refresh: false
# Auto-discovers baseline from S3, fetches only modified records
```

**Full Refresh (Fetch All Data)**
```yaml
config:
  full_refresh: true
  upload_to_s3: true
# Ignores baseline, fetches complete dataset from WordPress API
```

**Backfill from Specific Date**
```yaml
config:
  last_modified_date: "2024-01-15"
  upload_to_s3: true
# Fetches records modified since 2024-01-15, skips S3 auto-discovery
```

### Runtime Config Parameters

| Parameter | Type | Description | Default |
|-----------|------|-------------|---------|
| `upload_to_s3` | `bool` | Enable S3 upload after local Parquet export | `False` |
| `full_refresh` | `bool` | Bypass incremental logic and fetch all data | `False` |
| `last_modified_date` | `str` | Custom baseline date in YYYY-MM-DD format | `None` (auto-discover from S3) |
| `load_date` | `str` | Override partition date (YYYY-MM-DD) | Current date |
| `load_time` | `str` | Override partition time (HH:MM:SS) | Current time |

**Configuration Precedence**: `full_refresh` > `last_modified_date` > S3 auto-discovery > default baseline

**Incremental Load Parameters:**
- `last_modified_date`: User-friendly YYYY-MM-DD format, automatically converted to ISO 8601 datetime
- `full_refresh`: Set to `true` to bypass incremental logic and fetch all data. Default is `false`.
- If both are omitted, the schedule automatically discovers baseline from S3 partitions

#### Schedule Monitoring
The `bulletin_daily_schedule` automatically runs daily and monitors the WordPress API for content changes:
- Runs every day at 10:00 AM UTC (configurable via `cron_schedule` in [`sensors.py`](src/de_datalake_bulletin_dataload/defs/sensors.py))
- Checks for new/modified content by comparing latest API timestamps with cursor state
- Triggers incremental asset materialization when new data is detected
- Passes `last_modified_date` to assets for filtered API queries
- Tracks state using cursor-based persistence (format: `pages_modified|media_modified`)
- Skips execution if no new data since last run
- Uses `context.resources.get_config` for configuration (requires dagster-postgres for cursor storage)

## Project Structure
```
de-datalake-bulletin-dataload/
├── config/
│   └── fetch_config.json        # Endpoint configuration and API settings
├── src/
│   └── de_datalake_bulletin_dataload/
│       ├── definitions.py       # Main Dagster definitions and resources
│       └── defs/
│           ├── assets.py        # Asset definitions (pages, media endpoints)
│           ├── sensors.py       # Automated content change detection
│           ├── resources.py     # Configurable resources (Config, HTTP, Parquet, S3)
│           ├── validators.py    # Pydantic validation schemas
│           └── data_exporters.py # Parquet and S3 export functions
├── bulletin_raw/
│   └── endpoint_name/                 # Parquet export output directory
├── pyproject.toml               # Project dependencies and metadata
├── .env                         # Environment variables (not in git)
└── README.md                    # This file
```

## Architecture

### Assets
- **bulletin_{endpoint_name}**: Fetches WordPress data, validates, exports to Parquet

### Resources
- **ConfigResource**: Loads configuration from JSON with validation (base_url, endpoints, pagination, sensor_param)
- **HTTPClientResource**: Builds API URLs with proper pagination (delegates to ConfigResource for base_url)
- **ParquetExportResource**: Manages Parquet file paths with timestamp partitioning
- **AWSS3Resource**: Handles S3 uploads with lazy client initialization and credential validation

### Data Flow
1. Sensor monitors WordPress API for content changes (every 24 hours, cursor-based state tracking)
2. When changes detected, triggers asset materialization
3. Assets fetch data asynchronously with httpx (HTTP/2, 50 max connections, 20 keepalive)
4. Retry logic with exponential backoff (5s → 10s → 20s → 40s → 60s, up to 5 attempts)
5. Responses validated against endpoint-specific Pydantic schemas (Google Style docstrings)
6. Data exported to Parquet with Snappy compression, with following schema: `id`, `dl_inserted_at`, `payload`, `dl_hash`
7. (Optional) Uploaded to S3 with path: `/bulletin_raw/{endpoint}/load_date={date}/load_time={time}/`

### Parquet Export Format
| Column | Type | Description |
|--------|------|-------------|
| `id` | Int64 | WordPress content ID |
| `dl_inserted_at` | Datetime | Timestamp of data extraction |
| `payload` | String | Full JSON response as string |
| `dl_hash` | String | SHA-256 hash of payload for integrity |