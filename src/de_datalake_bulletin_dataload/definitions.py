from dagster import Definitions, EnvVar
from de_datalake_bulletin_dataload.defs.resources import DuckDBResource, HTTPClientResource, ParquetExportResource, PostgresResource
from de_datalake_bulletin_dataload.defs.assets import fetch_bulletin_data, validate_bulletin_data, insert_bulletin_data_to_postgres, export_bulletin_data_to_parquet_postgres

defs = Definitions(
    assets=[fetch_bulletin_data, validate_bulletin_data, insert_bulletin_data_to_postgres, export_bulletin_data_to_parquet_postgres],
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
        ),
        "postgres_connection": PostgresResource(
            host=EnvVar("POSTGRES_HOST"),
            port=EnvVar.int("POSTGRES_PORT"),
            user=EnvVar("POSTGRES_USER"),
            password=EnvVar("POSTGRES_PASSWORD"),
            database=EnvVar("POSTGRES_DB")
        )
    }
)