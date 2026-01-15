<h1 align="center">Bulletin Data DAG</h1>

<h2 align="center"><strong>An asynchronous DAG for loading BU Bulletin data</strong></h2>

<div align="center">

[![Version](https://img.shields.io/badge/Version-1.2.0-black.svg?logo=semanticrelease&logoColor=white)]()
[![Status](https://img.shields.io/badge/Status-Stable-darkgreen.svg?logo=progress&logoColor=white)]()
[![Changelog](https://img.shields.io/badge/Changelog-View-blue.svg?logo=readthedocs&logoColor=white)](./CHANGELOG.md)

[![Python](https://img.shields.io/badge/Python-3.13.x-3776AB.svg?logo=python&logoColor=white)](https://www.python.org/downloads/)
[![Dagster Docs](https://img.shields.io/badge/Dagster-1.12.x-6352ff)](https://docs.dagster.io)
</div>

## DAG Overview


## Dependencies
All Python runtime dependencies are defined in [`pyproject.toml`](pyproject.toml).  
The following package versions are verified for production use in the latest build:

### Core Dependencies
| Library             | Version    | Notes                                        |
|---------------------|------------|----------------------------------------------|
| `dagster`           | `1.12.10`  | Core orchestration framework                 |
| `duckdb`            | `>=1.4.3`  | Embedded analytical database                 |
| `httpx`             | `>=0.28.1` | Async HTTP client for API requests           |
| `pydantic`          | `>=2.12.5` | Data validation and schema enforcement       |
| `asyncio`           | `>=4.0.0`  | Asynchronous I/O support                     |

### Development Dependencies
Development tools are defined in `[dependency-groups]`:

| Library              | Notes                                        |
|----------------------|----------------------------------------------|
| `dagster-webserver`  | Local Dagster UI for pipeline visualization  |
| `dagster-dg-cli`     | Dagster CLI tools                            |

To install all dependencies including development tools, run:
```bash
uv sync --frozen
```

## 🌱 Environment Variables
This project requires several environment variables to be provided at runtime.  
For **local development**, use a `.env` file in the project root.  
For **production deployments**, use a Kubernetes Secrets resource or an approved institutional secrets manager.

### Required Variables
```env
BULLETIN_PAGES_WP_BASE_URL="https://www.bu.edu/academics/wp-json/wp/v2/pages?per_page=100&page="
USER_AGENT="de-dg-datalake-bulletin-dataload-1.1.1"
```

### Installation
1. Clone the repository
2. Install dependencies using `uv`:
   ```bash
   uv sync --frozen
   ```
3. Create a `.env` file in the project root with the required environment variables
4. Run the Dagster development server:
   ```bash
   uv run dg dev
   ```
5. Open the Dagster UI at `http://127.0.0.1:3000`

### Running the Pipeline
- **Via UI**: Navigate to the Assets page and click "Materialize all" or select individual assets
- **Via CLI**: 
  ```bash
  uv run dagster asset materialize -m de_datalake_bulletin_dataload.definitions
  ```

## Project Structure
```
de-datalake-bulletin-dataload/
├── src/
│   └── de_datalake_bulletin_dataload/
│       ├── definitions.py           # Main Dagster definitions and resources
│       └── defs/
│           ├── assets.py            # Asset definitions for the ETL pipeline
│           └── resources.py         # Configurable resources (DuckDB, HTTP, Parquet)
├── data/
│   ├── main/                        # DuckDB database storage
│   └── parquet/                     # Parquet export output
├── tests/                           # Unit tests (to be implemented)
├── pyproject.toml                   # Project dependencies and metadata
├── .env                             # Environment variables (not in git)
└── README.md                        # This fileQUET_EXPORT_FILE_PATH="./data/parquet/de-datalake-bulletin-data.parquet"
```

| Variable | Description |
|----------|-------------|
| `BULLETIN_PAGES_WP_BASE_URL` | WordPress REST API endpoint for bulletin pages (with pagination parameter) |
| `USER_AGENT` | User agent string for HTTP requests |
| `DUCKDB_DATABASE` | Path to DuckDB database file |
| `PARQUET_EXPORT_FILE_PATH` | Output path for Parquet export file |

### Notes
- The ETL architecture is fully **modular** and designed for **composable, reusable pipelines**.
- All configuration is designed to be **declarative**, supporting infrastructure-as-code workflows and reproducibility.
- **Shared utilities** and **resource definitions** ensure consistency and traceability across the ETL pipeline.

## Dockerfile
This project includes a production-optimized `Dockerfile` based on `python:3.12.11-slim-trixie`.

### Key Characteristics
- **Slim base image** minimizes container size while supporting native PostgreSQL drivers (`psycopg`).
- Temporary build dependencies (`libpq-dev`, `build-essential`) are used to compile native extensions and are removed after build to reduce image footprint and attack surface.
- A startup command is provided to enable optional integration with **Dagster UI** containers if desired.
- A `.dockerignore` file is provided to ensure **clean, fast, reproducible image builds**.

### Important Notes

## Kubernetes
This project is designed to be run on a schedule in **Dagster**, executed with the `k8s_job_executor`.
The k8s run executor definition is in the `definitions.py` file.

### Key Characteristics

### Kubernetes Security Posture

### Deployment Checklist

## GitHub Actions

### CI/CD Pipeline Characteristics

### Deployment Model

### Security Notes