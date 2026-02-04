from dagster import Definitions, EnvVar
from de_datalake_bulletin_dataload.defs.resources import (
    ParquetExportResource,
    AWSS3Resource,
    ConfigResource,
)
from de_datalake_bulletin_dataload.defs.assets import (
    fetch_export_pages_data,
    fetch_export_media_data,
)
from de_datalake_bulletin_dataload.defs.sensors import bulletin_data_sensor

defs = Definitions(
    assets=[fetch_export_pages_data, fetch_export_media_data],
    sensors=[bulletin_data_sensor],
    resources={
        "get_config": ConfigResource(config_path=EnvVar("CONFIG_PATH")),
        "parquet_export_path": ParquetExportResource(
            export_folder_path=EnvVar("PARQUET_EXPORT_FOLDER_PATH"),
            parquet_file_name=EnvVar("PARQUET_EXPORT_FILE_NAME"),
        ),
        "aws_s3_config": AWSS3Resource(
            bucket_name=EnvVar("AWS_S3_BUCKET_NAME"),
            access_key_id=EnvVar("AWS_ACCESS_KEY_ID"),
            secret_access_key=EnvVar("AWS_SECRET_ACCESS_KEY"),
            region_name=EnvVar("AWS_REGION_NAME"),
        ),
    },
)
