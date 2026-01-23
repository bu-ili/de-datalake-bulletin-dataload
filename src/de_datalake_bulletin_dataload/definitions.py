from dagster import Definitions, EnvVar
from de_datalake_bulletin_dataload.defs.resources import HTTPClientResource, ParquetExportResource, AWSS3Resource
from de_datalake_bulletin_dataload.defs.assets import fetch_export_pages_data

defs = Definitions(
    assets=[fetch_export_pages_data],
    resources={
        "http_client": HTTPClientResource(
            user_agent=EnvVar("USER_AGENT"),
            base_url=EnvVar("BULLETIN_PAGES_WP_BASE_URL")
        ),
        "parquet_export_path": ParquetExportResource(
            export_folder_path=EnvVar("PARQUET_EXPORT_FOLDER_PATH"),
            parquet_file_name=EnvVar("PARQUET_EXPORT_FILE_NAME")
        ),
        "aws_s3_config": AWSS3Resource(
            bucket_name=EnvVar("AWS_S3_BUCKET_NAME"),
            access_key_id=EnvVar("AWS_ACCESS_KEY_ID"),
            secret_access_key=EnvVar("AWS_SECRET_ACCESS_KEY"),
            region_name=EnvVar("AWS_REGION_NAME")
        )
    }
)