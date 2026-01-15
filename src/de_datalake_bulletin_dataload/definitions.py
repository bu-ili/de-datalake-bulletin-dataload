from dagster import Definitions, EnvVar
from de_datalake_bulletin_dataload.defs.resources import DuckDBResource, HTTPClientResource, ParquetExportResource
from de_datalake_bulletin_dataload.defs.assets import fetch_bulletin_data, validate_bulletin_data, insert_bulletin_data_to_duckdb, export_bulletin_data_to_parquet

defs = Definitions(
    assets=[fetch_bulletin_data, validate_bulletin_data, insert_bulletin_data_to_duckdb, export_bulletin_data_to_parquet],
    resources={
        "duckdb_connection": DuckDBResource(
            database=EnvVar("DUCKDB_DATABASE")
        ),
        "http_client": HTTPClientResource(
            user_agent=EnvVar("USER_AGENT"),
            base_url=EnvVar("BULLETIN_PAGES_WP_BASE_URL")
        ),
        "parquet_export_path": ParquetExportResource(
            export_path=EnvVar("PARQUET_EXPORT_FILE_PATH")
        )
    }
)