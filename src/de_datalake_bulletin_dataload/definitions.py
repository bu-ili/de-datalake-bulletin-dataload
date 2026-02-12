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
from de_datalake_bulletin_dataload.defs.schedules import (
    bulletin_daily_schedule,
    bulletin_raw_job,
)

# Initialize config resource first - reads from CONFIG_PATH env var or defaults to /app/config/config.json
config_resource = ConfigResource(
    config_path="/app/config/config.json"
)

defs = Definitions(
    assets=[fetch_export_pages_data, fetch_export_media_data],
    schedules=[bulletin_daily_schedule],
    jobs=[bulletin_raw_job],
    resources={
        "get_config": config_resource,
        "parquet_export_path": ParquetExportResource(
            config_resource=config_resource,
            # All values read from config.json by default
        ),
        "aws_s3_config": AWSS3Resource(
            config_resource=config_resource,
            # Credentials from environment variables (required)
            access_key_id=EnvVar("AWS_ACCESS_KEY_ID"),
            secret_access_key=EnvVar("AWS_SECRET_ACCESS_KEY"),
            # Bucket name and region read from config.json by default
        ),
    },
)
