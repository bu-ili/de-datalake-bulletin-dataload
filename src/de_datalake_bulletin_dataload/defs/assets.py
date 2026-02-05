from dagster import asset, AssetExecutionContext, Config
import asyncio
import httpx
from datetime import datetime
import time
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type
)
from typing import Optional
from de_datalake_bulletin_dataload.defs.resources import (
    ConfigResource,
    ParquetExportResource,
    AWSS3Resource
)
from de_datalake_bulletin_dataload.defs.validators import validate_batch_responses
from de_datalake_bulletin_dataload.defs.data_exporters import (
    export_to_parquet,
    export_to_s3
)
from de_datalake_bulletin_dataload.defs.utilities import (
    get_last_modified_from_parquet,
    cleanup_old_local_files
)


class RuntimeConfig(Config):
    """
    Runtime configuration for assets that fetch data from the BU Bulletin Wordpress API.

    Attributes:
        upload_to_s3 (bool): Flag to enable/disable S3 upload. Default is False for testing.
        load_date (str): Date string for partitioning (YYYY-MM-DD). Defaults to current date.
        load_time (str): Time string for partitioning (HH:MM:SS). Defaults to current time.
        last_modified_date (str): ISO 8601 datetime string for filtering records modified after this date.
                                  Enables incremental loads. If None, fetches all data (full refresh).
        full_refresh (bool): When True, bypasses incremental logic and fetches all data regardless
                           of last_modified_date. Default is False.
    """

    upload_to_s3: bool = False
    load_date: Optional[str] = None
    load_time: Optional[str] = None
    last_modified_date: Optional[str] = None
    full_refresh: bool = False


@retry(
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=5, max=60),
    retry=retry_if_exception_type((httpx.HTTPError, httpx.HTTPStatusError)),
    reraise=True,
)
async def fetch_page(
    session: httpx.AsyncClient,
    page_number: int,
    total_pages: int,
    base_url: str,
    headers: dict,
    context: AssetExecutionContext,
) -> list:
    """
    Fetch a single page of data from the Wordpress Bulletin API asynchronously.

    Retries up to 5 times with exponential backoff (5s, 10s, 20s, 40s, 60s) using Tenacity.

    Args:
        session (httpx.AsyncClient): httpx AsyncClient session.
        page_number (int): Page number to fetch.
        total_pages (int): Total number of pages to fetch.
        base_url (str): Base URL to fetch the data from (with pagination param).
        headers (dict): Headers to include in the request.
        context (AssetExecutionContext): Dagster AssetExecutionContext for logging.

    Returns:
        list: API response data for the requested page.

    Raises:
        httpx.HTTPStatusError: If non-200 status code after all retries.
        httpx.HTTPError: If other HTTP errors occur after all retries.
    """
    try:
        context.log.info(f"Fetching page {page_number} of {total_pages}")
        response = await session.get(url=f"{base_url}{page_number}", headers=headers)
        response.raise_for_status()
        page_data = response.json()
        context.log.info(f"Page {page_number} fetched with {len(page_data)} records")
        return page_data
    except (httpx.HTTPError, httpx.HTTPStatusError) as e:
        context.log.warning(
            f"Error fetching page {page_number}: {e}. Tenacity will retry..."
        )
        raise
    except Exception as e:
        context.log.error(f"Unexpected error fetching page {page_number}: {e}")
        return []


async def fetch_all_pages(
    get_config: ConfigResource,
    endpoint_key: str,
    context: AssetExecutionContext,
    config: RuntimeConfig,
    parquet_export_path: ParquetExportResource,
) -> list:
    """Fetch all pages concurrently from the Wordpress Bulletin API.

    Utilizes httpx with HTTP/2 for improved performance and asyncio for concurrency.
    Supports incremental loads via modified_after filter when last_modified_date is provided.

    Args:
        get_config (ConfigResource): ConfigResource containing configuration and HTTP settings.
        endpoint_key (str): The endpoint key to fetch data from (e.g., 'pages', 'media').
        context (AssetExecutionContext): Dagster AssetExecutionContext for logging.
        config (RuntimeConfig): Runtime configuration with last_modified_date and full_refresh flags.
        parquet_export_path (ParquetExportResource): Resource for Parquet export path.

    Returns:
        list: API response data for all available pages. Empty list if no new data since last_modified_date.

    Raises:
        Exception: If any error occurs during the HTTP request.
    """
    base_url = get_config.get_all_endpoint_urls()[endpoint_key]
    default_baseline = get_config.get_config_value("default_baseline_datetime", required=True)
    idempotency_param = get_config.get_config_value("idempotency_param", required=True)
    loop_pagination_param = get_config.get_config_value("loop_pagination_param", required=True)

    if config.full_refresh:
        context.log.info(f"Full refresh: Fetching all {endpoint_key} data")
        filter_date = None
    elif config.last_modified_date:
        context.log.info(f"Incremental load: Using provided date {config.last_modified_date}")
        filter_date = config.last_modified_date
    else:
        filter_date = get_last_modified_from_parquet(
            endpoint_key, parquet_export_path.export_folder_path, get_config
        )
        context.log.info(
            f"Incremental load: Auto-discovered baseline from local Parquet: {filter_date}"
        )

    if filter_date and filter_date != default_baseline:
        request_base_url = f"{base_url}{idempotency_param}{filter_date}{loop_pagination_param}"
    else:
        request_base_url = f"{base_url}{loop_pagination_param}"

    client_config = get_config.get_http_client_config()
    headers = get_config.get_headers()

    async with httpx.AsyncClient(**client_config) as session:
        response = await session.get(url=f"{request_base_url}1", headers=headers)
        total_pages = int(response.headers.get("X-WP-TotalPages", 1))
        first_page_data = response.json()
        
        if not isinstance(first_page_data, list):
            context.log.error(f"Unexpected API response type: {type(first_page_data)}. Expected list, got: {first_page_data}")
            return []

        context.log.info(f"Total pages to fetch for '{endpoint_key}': {total_pages}")

        if total_pages > 1:
            tasks = [
                fetch_page(session, page_num, total_pages, request_base_url, headers, context)
                for page_num in range(2, total_pages + 1)
            ]
            remaining_pages = await asyncio.gather(*tasks, return_exceptions=False)
            all_data = [item for item in first_page_data]
            all_data.extend(
                [item for page_data in remaining_pages for item in page_data]
            )
        else:
            all_data = first_page_data
        
        for idx, item in enumerate(all_data):
            if not isinstance(item, dict):
                context.log.error(f"Item {idx} is not a dict: type={type(item)}, value={item}")
                return []

        return all_data


@asset(
    name="bulletin_raw_pages",
    group_name="bulletin_raw",
)
async def fetch_export_pages_data(
    context: AssetExecutionContext,
    config: RuntimeConfig,
    get_config: ConfigResource,
    parquet_export_path: ParquetExportResource,
    aws_s3_config: AWSS3Resource,
) -> str:
    """
    Fetch pages data from Bulletin API, validate, export to Parquet, and upload to S3.

    Utilizes asynchronous HTTP requests via httpx, validates using pydantic models,
    exports using Polars, and uploads to AWS S3 if configured.

    Args:
        context (AssetExecutionContext): Dagster AssetExecutionContext for logging.
        config (RuntimeConfig): Runtime configuration for partitioning and S3 upload.
        get_config (ConfigResource): ConfigResource for configuration and HTTP settings.
        parquet_export_path (ParquetExportResource): ParquetExportResource for export settings.
        aws_s3_config (AWSS3Resource): AWSS3Resource for managing AWS S3 connections.

    Returns:
        str: Path to the exported Parquet file or S3 location.

    Raises:
        Exception: If any error occurs during fetch, validation, export, or upload.
    """
    start_time = time.perf_counter()
    endpoint_key = "pages"

    load_date = config.load_date or datetime.now().strftime("%Y-%m-%d")
    load_time = config.load_time or datetime.now().strftime("%H:%M:%S")

    context.log.info("Starting bulletin pages data fetch from WordPress API")

    data = await fetch_all_pages(
        get_config, endpoint_key, context, config, parquet_export_path
    )

    # Handle empty response (no new data since last modified date)
    if not data:
        context.log.info(
            f"No new {endpoint_key} records since {config.last_modified_date or 'last run'}"
        )
        return f"0 records processed (no updates since {config.last_modified_date or 'last run'})"

    validated_response = validate_batch_responses(data, endpoint_key, context)
    validated_data = [item.model_dump(by_alias=True) for item in validated_response]
    context.log.info(f"Validated {len(validated_data)} pages records")

    export_path = export_to_parquet(
        export_path=parquet_export_path,
        validated_data=validated_data,
        endpoint_key=endpoint_key,
        load_date=load_date,
        load_time=load_time,
        context=context,
    )
    context.log.info(f"Exported pages data to Parquet: {export_path}")

    if config.upload_to_s3:
        aws_s3_path = export_to_s3(
            aws_s3_config=aws_s3_config, file_path=export_path, context=context
        )
        context.log.info(f"Uploaded pages data to S3: {aws_s3_path}")
        
        # Cleanup old local files after successful S3 upload
        cleanup_old_local_files(
            current_file_path=export_path,
            get_config=config,
            keep_versions=2,
            context=context,
        )
        
        result_path = aws_s3_path
    else:
        context.log.info("Skipping S3 upload for pages as per configuration")
        result_path = export_path

    total_time = time.perf_counter() - start_time
    context.log.info(
        f"Total time for pages fetch, validate, export, and upload: {total_time:.2f} seconds"
    )

    return result_path


@asset(
    name="bulletin_raw_media",
    group_name="bulletin_raw",
)
async def fetch_export_media_data(
    context: AssetExecutionContext,
    config: RuntimeConfig,
    get_config: ConfigResource,
    parquet_export_path: ParquetExportResource,
    aws_s3_config: AWSS3Resource,
) -> str:
    """
    Fetch media data from Bulletin API, validate, export to Parquet, and upload to S3.

    Utilizes asynchronous HTTP requests via httpx, validates using pydantic models,
    exports using Polars, and uploads to AWS S3 if configured.

    Args:
        context (AssetExecutionContext): Dagster AssetExecutionContext for logging.
        config (RuntimeConfig): Runtime configuration for partitioning and S3 upload.
        get_config (ConfigResource): ConfigResource for configuration and HTTP settings.
        parquet_export_path (ParquetExportResource): ParquetExportResource for export settings.
        aws_s3_config (AWSS3Resource): AWSS3Resource for managing AWS S3 connections.

    Returns:
        str: Path to the exported Parquet file or S3 location.

    Raises:
        Exception: If any error occurs during fetch, validation, export, or upload.
    """
    start_time = time.perf_counter()
    endpoint_key = "media"

    load_date = config.load_date or datetime.now().strftime("%Y-%m-%d")
    load_time = config.load_time or datetime.now().strftime("%H:%M:%S")

    context.log.info("Starting bulletin media data fetch from WordPress API")

    data = await fetch_all_pages(
        get_config, endpoint_key, context, config, parquet_export_path
    )

    # Handle empty response (no new data since last modified date)
    if not data:
        context.log.info(
            f"No new {endpoint_key} records since {config.last_modified_date or 'last run'}"
        )
        return f"0 records processed (no updates since {config.last_modified_date or 'last run'})"

    validated_response = validate_batch_responses(data, endpoint_key, context)
    validated_data = [item.model_dump(by_alias=True) for item in validated_response]
    context.log.info(f"Validated {len(validated_data)} media records")

    export_path = export_to_parquet(
        export_path=parquet_export_path,
        validated_data=validated_data,
        endpoint_key=endpoint_key,
        load_date=load_date,
        load_time=load_time,
        context=context,
    )
    context.log.info(f"Exported media data to Parquet: {export_path}")

    if config.upload_to_s3:
        aws_s3_path = export_to_s3(
            aws_s3_config=aws_s3_config, file_path=export_path, context=context
        )
        context.log.info(f"Uploaded media data to S3: {aws_s3_path}")
        
        # Cleanup old local files after successful S3 upload
        cleanup_old_local_files(
            current_file_path=export_path,
            get_config=config,
            keep_versions=2,
            context=context,
        )
        
        result_path = aws_s3_path
    else:
        context.log.info("Skipping S3 upload for media as per configuration")
        result_path = export_path

    total_time = time.perf_counter() - start_time
    context.log.info(
        f"Total time for media fetch, validate, export, and upload: {total_time:.2f} seconds"
    )

    return result_path
