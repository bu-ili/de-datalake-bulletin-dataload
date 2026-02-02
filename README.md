<h1 align="center">Bulletin Data DAG</h1>

<h2 align="center"><strong>An asynchronous DAG for loading BU Bulletin data</strong></h2>

<div align="center">

[![Version](https://img.shields.io/badge/Version-1.2.0-black.svg?logo=semanticrelease&logoColor=white)]()
[![Status](https://img.shields.io/badge/Status-Testing-orange.svg?logo=progress&logoColor=white)]()
[![Changelog](https://img.shields.io/badge/Changelog-View-blue.svg?logo=readthedocs&logoColor=white)](./CHANGELOG.md)

[![Python](https://img.shields.io/badge/Python-3.13.x-3776AB.svg?logo=python&logoColor=white)](https://www.python.org/downloads/)
[![Dagster Docs](https://img.shields.io/badge/Dagster-1.12.x-6352ff)](https://docs.dagster.io)
</div>

## Overview

This Dagster pipeline fetches data from the BU Bulletin WordPress API, validates responses, exports to Parquet format, and uploads to AWS S3. It includes a sensor that automatically detects new or modified content and triggers data refreshes.

### Key Features
- **Multi-endpoint support**: Separate assets for pages and media endpoints
- **Automated monitoring**: Sensor checks for content changes and triggers refreshes (daily)
- **Optimized async HTTP**: HTTP/2 with 50 concurrent connections and exponential backoff (5-60s, 5 attempts)
- **Data validation**: Endpoint-specific Pydantic schemas with Google Style docstrings
- **Configurable exports**: Parquet files with hash integrity checks and Snappy compression
- **Optional S3 upload**: Runtime toggle for cloud storage integration
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

### Environment Variables (.env)
This project uses environment variables for runtime configuration and secrets management.  
For **local development**, use a `.env` file in the project root.  
For **production deployments**, use Kubernetes Secrets.

#### Core Configuration
```env
USER_AGENT="your-project-name-version"
CONFIG_PATH="/app/config/config.json"
```

| Variable | Description | Required |
|----------|-------------|----------|
| `USER_AGENT` | User agent string for HTTP requests | Yes |
| `CONFIG_PATH` | Path to config.json file (contains base_url) | Yes |

#### Parquet Export Configuration
```env
PARQUET_EXPORT_FOLDER_PATH="./data/parquet/"
PARQUET_EXPORT_FILE_NAME="endpoint_export_data.parquet"
```

| Variable | Description | Required |
|----------|-------------|----------|
| `PARQUET_EXPORT_FOLDER_PATH` | Base directory for Parquet file exports | Yes |
| `PARQUET_EXPORT_FILE_NAME` | Filename for Parquet exports | Yes |

#### AWS S3 Configuration (Optional)
```env
AWS_S3_BUCKET_NAME="your-bucket-name"
AWS_ACCESS_KEY_ID="your-access-key"
AWS_SECRET_ACCESS_KEY="your-secret-key"
AWS_REGION_NAME="region-name"
```

| Variable | Description | Required |
|----------|-------------|----------|
| `AWS_S3_BUCKET_NAME` | S3 bucket name for data uploads | Only if `upload_to_s3=true` |
| `AWS_ACCESS_KEY_ID` | AWS access key | Only if `upload_to_s3=true` |
| `AWS_SECRET_ACCESS_KEY` | AWS secret key | Only if `upload_to_s3=true` |
| `AWS_REGION_NAME` | AWS region (e.g., us-east-1) | Only if `upload_to_s3=true` |

**Note**: S3 upload is controlled by the `upload_to_s3` runtime flag in asset materialization config.

### API Configuration (config.json)
The [`config/config.json`](config/config.json) file defines WordPress API endpoint behavior. This allows you to add or modify endpoints without changing code.

**Example configuration:**
```json
{
    "base_url": "https://example.com/wp-json/wp/v2/",
    "endpoints": {
        "pages": "pages?",
        "media": "media?"
    },
    "pagination": "per_page=value&page=",
    "sensor_param": "?orderby=modified&order=desc&per_page=1"
}
```

| Field | Description | Usage |
|-------|-------------|-------|
| `base_url` | WordPress REST API base URL | Base URL for all API requests |
| `endpoints` | Dictionary of endpoint keys and their API paths | Each key creates a fetchable endpoint |
| `pagination` | URL parameters for paginated requests | Appended to endpoint URL during data fetching (page number added dynamically) |
| `sensor_param` | Query parameters for sensor monitoring | Used to fetch only the latest modified date efficiently |

**How it works:**
1. **ConfigResource** loads this file at runtime via `CONFIG_PATH`
2. **HTTPClientResource** builds full URLs: `{base_url}{endpoint}{pagination}{page_num}`
   - Example: `https://example.com/wp-json/wp/v2/endpoint1?path_parameter=value`
3. **Sensor** uses `sensor_param` to check for new content:
   - Example: `https://example.com/wp-json/wp/v2/endpoint1?path_parameter=value`
4. **Assets** are automatically created for each endpoint key in the `endpoints` dictionary

**Adding new endpoints:**
Simply add a new key-value pair to the `endpoints` object, create corresponding assets in [assets.py](src/de_datalake_bulletin_dataload/defs/assets.py), and add validation schemas in [validators.py](src/de_datalake_bulletin_dataload/defs/validators.py).

## Installation & Usage

### Installation
1. Clone the repository
2. Install dependencies using `uv`:
   ```bash
   uv sync --frozen
   ```
3. (Optional) Create a `.env` file in the project root for AWS credentials if using S3 upload
4. Configure endpoints in [`config/fetch_config.json`](config/fetch_config.json)

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
```

#### Sensor Monitoring
The `bulletin_data_sensor` automatically monitors the WordPress API for content changes:
- Checks every 24 hours (86000 seconds, configurable in [`sensors.py`](src/de_datalake_bulletin_dataload/defs/sensors.py))
- Triggers asset materialization when new or modified content is detected
- Tracks state using cursor-based persistence (format: `pages_modified|media_modified`)
- Uses `context.resources.get_config` for configuration (requires dagster-postgres)

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