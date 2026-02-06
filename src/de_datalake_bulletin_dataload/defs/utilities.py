import re
from typing import Optional, Annotated
from pydantic import Field
from dagster import AssetExecutionContext, Config
from de_datalake_bulletin_dataload.defs.resources import ConfigResource, AWSS3Resource


class RuntimeConfig(Config):
    """
    Runtime configuration for assets that fetch data from the BU Bulletin Wordpress API.
    For incremental loads, the filter date is determined by the following logic:
    1. If full_refresh=True → return None (fetch all data)
    2. If last_modified_date is provided → use it
3. Otherwise → auto-discover from S3 partitions
    """

    upload_to_s3: Annotated[
        bool,
        Field(
            description="Enable S3 upload after exporting to local Parquet (default: False for testing)"
        ),
    ] = False

    load_date: Annotated[
        Optional[str],
        Field(
            description="Date for partitioning in YYYY-MM-DD format (default: current date)"
        ),
    ] = None

    load_time: Annotated[
        Optional[str],
        Field(
            description="Time for partitioning in HH:MM:SS format (default: current time)"
        ),
    ] = None

    last_modified_date: Annotated[
        Optional[str],
        Field(
            description="Baseline date in YYYY-MM-DD format for incremental loads (default: auto-discover from S3)"
        ),
    ] = None

    full_refresh: Annotated[
        bool,
        Field(
            description="Bypass incremental logic and fetch all data (default: False)"
        ),
    ] = False


def get_last_modified_from_s3(
    endpoint: str,
    aws_s3_config: AWSS3Resource,
    get_config: ConfigResource,
    context: AssetExecutionContext,
) -> str:
    """Query S3 to find the most recent load_date partition for an endpoint.

    Lists objects in bulletin_raw/{endpoint}/ and finds the most recently uploaded file
    based on S3's LastModified timestamp. Extracts the load_date partition from the key.

    Args:
        endpoint (str): API endpoint key (e.g., 'pages', 'media').
        aws_s3_config (AWSS3Resource): S3 resource for accessing bucket.
        get_config (ConfigResource): ConfigResource for getting configuration constants.
        context (AssetExecutionContext): For logging.

    Returns:
        str: Date string (YYYY-MM-DD) from the most recent partition, or default baseline if no files found.
    """
    default_baseline = get_config.get_config_value("default_baseline_datetime")
    partition_date_prefix = get_config.get_config_value("partition_date_prefix")

    try:
        s3_client = aws_s3_config.get_s3_client()
        bucket = aws_s3_config.bucket_name
        prefix = f"bulletin_raw/{endpoint}/"

        context.log.info(
            f"Querying S3 for most recent partition in s3://{bucket}/{prefix}"
        )

        response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)

        if "Contents" not in response or not response["Contents"]:
            context.log.info(
                f"No S3 objects found for {endpoint}, using default baseline"
            )
            return default_baseline

        # Get most recently modified object
        most_recent = max(response["Contents"], key=lambda x: x["LastModified"])
        key = most_recent["Key"]

        # Extract load_date from key: bulletin_raw/pages/load_date=2024-01-15/load_time=.../file.parquet
        pattern = rf"{partition_date_prefix}(\d{{4}}-\d{{2}}-\d{{2}})"
        match = re.search(pattern, key)

        if match:
            baseline_date = match.group(1)
            context.log.info(
                f"Found baseline for {endpoint}: {baseline_date} (from {key})"
            )
            return baseline_date
        else:
            context.log.warning(f"Could not extract date from S3 partition: {key}")
            return default_baseline

    except Exception as e:
        context.log.error(f"Error querying S3 for {endpoint}: {e}")
        return default_baseline


def determine_filter_date(
    endpoint_key: str,
    config: RuntimeConfig,
    aws_s3_config: AWSS3Resource,
    get_config: ConfigResource,
    context: AssetExecutionContext,
) -> Optional[str]:
    """
    Determine the filter date for incremental data loading.

    Decision logic:
    1. If full_refresh=True → return None (fetch all data)
    2. If last_modified_date is provided → use it
    3. Otherwise → auto-discover from S3 partitions

    Args:
        endpoint_key (str): API endpoint key (e.g., 'pages', 'media').
        config (RuntimeConfig): Runtime configuration with full_refresh and last_modified_date.
        aws_s3_config (AWSS3Resource): S3 resource for querying buckets.
        get_config (ConfigResource): ConfigResource for getting configuration constants.
        context (AssetExecutionContext): For logging.

    Returns:
        Optional[str]: Date string (YYYY-MM-DD) to filter by, or None for full refresh.
    """
    if config.full_refresh:
        context.log.info(
            f"Full refresh: Fetching all {endpoint_key} data (full_refresh=True)"
        )
        return None

    if config.last_modified_date:
        # Transform YYYY-MM-DD to ISO 8601 datetime format (YYYY-MM-DDTHH:MM:SS)
        date_str = config.last_modified_date
        if "T" not in date_str:  # If not already ISO 8601 format
            date_str = f"{date_str}T00:00:00"
        context.log.info(
            f"Incremental load: Using provided date {date_str} for {endpoint_key}"
        )
        return date_str

    # Auto-discover from S3 partitions
    filter_date = get_last_modified_from_s3(
        endpoint_key, aws_s3_config, get_config, context
    )
    
    # Transform S3-discovered date to ISO 8601 format if needed
    if filter_date:
        filter_date = f"{filter_date}T00:00:00"
    
    context.log.info(
        f"Incremental load: Auto-discovered baseline from S3 for {endpoint_key}: {filter_date}"
    )
    return filter_date
