from dagster import asset, AssetExecutionContext, Config
import asyncio
import httpx
from datetime import datetime
import time
import json
import polars as pl
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from typing import Optional
from de_datalake_bulletin_dataload.defs.resources import HTTPClientResource, ParquetExportResource, AWSS3Resource
from de_datalake_bulletin_dataload.defs.validators import validate_batch_responses
from de_datalake_bulletin_dataload.defs.data_exporters import export_to_parquet, export_to_s3


class RuntimeConfig(Config):
    """
    Runtime configuration for assets that fetch data from the BU Bulletin Wordpress API.
    
    Attributes:
        upload_to_s3 (bool): Flag to enable/disable S3 upload. Default is False for testing.
        load_date (str): Date string for partitioning (YYYY-MM-DD). Defaults to current date.
        load_time (str): Time string for partitioning (HH:MM:SS). Defaults to current time.
    """
    upload_to_s3: bool = False
    load_date: Optional[str] = None
    load_time: Optional[str] = None



@retry(
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=5, max=60),
    retry=retry_if_exception_type((httpx.HTTPError, httpx.HTTPStatusError)),
    reraise=True
)
async def fetch_page(session: httpx.AsyncClient, page_number: int, total_pages: int, base_url: str, headers: dict, context: AssetExecutionContext) -> list:
    """
    Fetch a single page of data from the Wordpress Bulletin API asynchronously.
    
    Retries up to 5 times with exponential backoff (5s, 10s, 20s, 40s, 60s) using Tenacity.
    
    Args:
        session (httpx.AsyncClient): httpx AsyncClient session.
        page_number (int): Page number to fetch.
        total_pages (int): Total number of pages to fetch.
        base_url (str): Base URL to fetch the data from.
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
        context.log.warning(f"Error fetching page {page_number}: {e}. Tenacity will retry...")
        raise 
    except Exception as e:
        context.log.error(f"Unexpected error fetching page {page_number}: {e}")
        return []


async def fetch_all_pages(http_client: HTTPClientResource, endpoint_key: str, context: AssetExecutionContext) -> list:
    """
    Fetch all pages concurrently from the Wordpress Bulletin API.
    
    Utilizes httpx with HTTP/2 for improved performance and asyncio for concurrency.
    
    Args:
        http_client (HTTPClientResource): HTTPClientResource containing base URL and headers.
        endpoint_key (str): The endpoint key to fetch data from (e.g., 'pages', 'media').
        context (AssetExecutionContext): Dagster AssetExecutionContext for logging.
    
    Returns:
        list: API response data for all available pages.
    
    Raises:
        Exception: If any error occurs during the HTTP request.
    """
    base_url = http_client.get_all_endpoint_urls()[endpoint_key]
    limits = httpx.Limits(max_connections=50, max_keepalive_connections=20)
    timeout = httpx.Timeout(30.0, connect=10.0)
    async with httpx.AsyncClient(limits=limits, timeout=timeout, http2=True) as session:
        response = await session.get(url=f"{base_url}1", headers=http_client.get_headers())
        total_pages = int(response.headers.get("X-WP-TotalPages", 1))
        first_page_data = response.json()

        context.log.info(f"Total pages to fetch for '{endpoint_key}': {total_pages}")

        tasks = [
            fetch_page(session, page_num, total_pages, base_url, http_client.get_headers(), context)
            for page_num in range(1, total_pages + 1)
        ]
        
        all_pages = await asyncio.gather(*tasks, return_exceptions=False)

        all_data = [item for page_data in all_pages for item in page_data]
        
        return all_data
    
async def fetch_multiple_endpoints(http_client: HTTPClientResource, endpoint_keys: list[str], context: AssetExecutionContext) -> dict[str, list]:
    """
    Fetch data from multiple endpoints concurrently.
    
    Utilizes asyncio for concurrency and fetch_all_pages for individual endpoint retrieval.
    
    Args:
        http_client (HTTPClientResource): HTTPClientResource containing base URL and headers.
        endpoint_keys (list[str]): List of endpoint keys to fetch (e.g., ['pages', 'media']).
        context (AssetExecutionContext): Dagster AssetExecutionContext for logging.
    
    Returns:
        dict: Dictionary with endpoint_key as key and fetched data as value.
    
    Raises:
        Exception: If any error occurs during the HTTP requests.
    """
    context.log.info(f"Fetching data from {len(endpoint_keys)} endpoints concurrently: {endpoint_keys}")
    
    # Fetch all endpoints concurrently
    tasks = [
        fetch_all_pages(http_client, endpoint_key, context)
        for endpoint_key in endpoint_keys
    ]
    
    results = await asyncio.gather(*tasks)
    
    # Map results to endpoint keys
    endpoint_data = {
        endpoint_key: data 
        for endpoint_key, data in zip(endpoint_keys, results)
    }
    
    for endpoint_key, data in endpoint_data.items():
        context.log.info(f"Endpoint '{endpoint_key}': Fetched {len(data)} total records")
    
    return endpoint_data


@asset(
    name="bulletin_pages",
    group_name="de_datalake_bulletin",
)
async def fetch_export_pages_data(context: AssetExecutionContext, config: RuntimeConfig, http_client: HTTPClientResource, parquet_export_path: ParquetExportResource, aws_s3_config: AWSS3Resource) -> str:
    """
    Fetch pages data from Bulletin API, validate, export to Parquet, and upload to S3.
    
    Utilizes asynchronous HTTP requests via httpx, validates using pydantic models,
    exports using Polars, and uploads to AWS S3 if configured.

    Args:
        context (AssetExecutionContext): Dagster AssetExecutionContext for logging.
        config (RuntimeConfig): Runtime configuration for partitioning and S3 upload.
        http_client (HTTPClientResource): HTTPClientResource for making API requests.
        parquet_export_path (ParquetExportResource): ParquetExportResource for export settings.
        aws_s3_config (AWSS3Resource): AWSS3Resource for managing AWS S3 connections.

    Returns:
        str: Path to the exported Parquet file or S3 location.

    Raises:
        Exception: If any error occurs during fetch, validation, export, or upload.
    """
    start_time = time.perf_counter()
    endpoint_key = "pages"
    
    # Generate timestamps if not provided
    load_date = config.load_date or datetime.now().strftime("%Y-%m-%d")
    load_time = config.load_time or datetime.now().strftime("%H:%M:%S")
    
    context.log.info(f"Starting bulletin pages data fetch from WordPress API")
    
    # Fetch pages data
    data = await fetch_all_pages(http_client, endpoint_key, context)
    
    # Validate with pages-specific schema
    validated_response = validate_batch_responses(data, endpoint_key, context)
    validated_data = [item.model_dump(by_alias=True) for item in validated_response]
    context.log.info(f"Validated {len(validated_data)} pages records")
    
    # Export to Parquet
    export_path = export_to_parquet(
        export_path=parquet_export_path,
        validated_data=validated_data,
        endpoint_key=endpoint_key,
        load_date=load_date,
        load_time=load_time,
        context=context
    )
    context.log.info(f"Exported pages data to Parquet: {export_path}")
    
    if config.upload_to_s3:
        aws_s3_path = export_to_s3(
            aws_s3_config=aws_s3_config,
            file_path=export_path,
            context=context
        )
        context.log.info(f"Uploaded pages data to S3: {aws_s3_path}")
        result_path = aws_s3_path
    else:
        context.log.info(f"Skipping S3 upload for pages as per configuration")
        result_path = export_path
    
    total_time = time.perf_counter() - start_time
    context.log.info(f"Total time for pages fetch, validate, export, and upload: {total_time:.2f} seconds")
    
    return result_path


@asset(
    name="bulletin_media",
    group_name="de_datalake_bulletin",
)
async def fetch_export_media_data( context: AssetExecutionContext, config: RuntimeConfig, http_client: HTTPClientResource, parquet_export_path: ParquetExportResource, aws_s3_config: AWSS3Resource) -> str:
    """
    Fetch media data from Bulletin API, validate, export to Parquet, and upload to S3.
    
    Utilizes asynchronous HTTP requests via httpx, validates using pydantic models,
    exports using Polars, and uploads to AWS S3 if configured.
    
    Args:
        context (AssetExecutionContext): Dagster AssetExecutionContext for logging.
        config (RuntimeConfig): Runtime configuration for partitioning and S3 upload.
        http_client (HTTPClientResource): HTTPClientResource for making API requests.
        parquet_export_path (ParquetExportResource): ParquetExportResource for export settings.
        aws_s3_config (AWSS3Resource): AWSS3Resource for managing AWS S3 connections.

    Returns:
        str: Path to the exported Parquet file or S3 location.

    Raises:
        Exception: If any error occurs during fetch, validation, export, or upload.
    """
    start_time = time.perf_counter()
    endpoint_key = "media"
    
    # Generate timestamps if not provided
    load_date = config.load_date or datetime.now().strftime("%Y-%m-%d")
    load_time = config.load_time or datetime.now().strftime("%H:%M:%S")
    
    context.log.info(f"Starting bulletin media data fetch from WordPress API")
    
    data = await fetch_all_pages(http_client, endpoint_key, context)
    
    validated_response = validate_batch_responses(data, endpoint_key, context)
    validated_data = [item.model_dump(by_alias=True) for item in validated_response]
    context.log.info(f"Validated {len(validated_data)} media records")
    
    export_path = export_to_parquet(
        export_path=parquet_export_path,
        validated_data=validated_data,
        endpoint_key=endpoint_key,
        load_date=load_date,
        load_time=load_time,
        context=context
    )
    context.log.info(f"Exported media data to Parquet: {export_path}")
    
    if config.upload_to_s3:
        aws_s3_path = export_to_s3(
            aws_s3_config=aws_s3_config,
            file_path=export_path,
            context=context
        )
        context.log.info(f"Uploaded media data to S3: {aws_s3_path}")
        result_path = aws_s3_path
    else:
        context.log.info(f"Skipping S3 upload for media as per configuration")
        result_path = export_path
    
    total_time = time.perf_counter() - start_time
    context.log.info(f"Total time for media fetch, validate, export, and upload: {total_time:.2f} seconds")
    
    return result_path
