import httpx
import glob
import shutil
import polars as pl
from pathlib import Path
from dagster import AssetExecutionContext
from de_datalake_bulletin_dataload.defs.resources import ConfigResource

def fetch_latest_modified_date(endpoint: str, get_config: ConfigResource) -> str:
    """Fetch the most recent modified date from Bulletin Wordpress API for an endpoint.

    The sensor parameter defines the query to get the latest modified record. The response
    is compared against the last known modified date stored in the schedule cursor.

    Args:
        endpoint (str): API endpoint to query (e.g., 'pages', 'media').
        get_config (ConfigResource): ConfigResource for getting parameter to build URL for finding latest modified record for each API endpoint.

    Returns:
        str: Datetime string of the latest modified date, or default baseline if fetch fails.
    """
    base_url = get_config.get_config_value("base_url", required=True)
    latest_modified_param = get_config.get_config_value("latest_modified_param", required=True)
    url = f"{base_url}{endpoint}{latest_modified_param}"
    
    default_baseline = get_config.get_config_value("default_baseline_datetime")

    try:
        response = httpx.get(url, timeout=30.0)
        response.raise_for_status()
        data = response.json()

        if data and len(data) > 0:
            return data[0]["modified"]
    except Exception as e:
        print(f"Error fetching latest modified date for {endpoint}: {e}")

    return default_baseline

def get_last_modified_from_parquet(
    endpoint: str, export_folder_path: str, get_config: ConfigResource
) -> str:
    """Read the most recent modified timestamp from existing local Parquet files.

    Reads from local Parquet files to establish baseline for incremental loads.
    Builds a glob pattern to find Parquet files for the given endpoint, reads the most recent Parquet file using polars,
    and extracts the maximum 'modified' timestamp if file is available.

    Args:
        endpoint (str): API endpoint to query (e.g., 'pages', 'media').
        export_folder_path (str): Base path where Parquet files are exported.
        get_config (ConfigResource): ConfigResource for getting configuration constants.

    Returns:
        str: Datetime string of the latest modified date, or default baseline if no files found.
    """
    default_baseline = get_config.get_config_value("default_baseline_datetime", "2000-01-01T00:00:00")
    partition_date_prefix = get_config.get_config_value("partition_date_prefix", "load_date=")
    partition_time_prefix = get_config.get_config_value("partition_time_prefix", "load_time=")
    parquet_extension = get_config.get_config_value("parquet_file_extension", "*.parquet")

    try:
        # Build glob pattern for endpoint parquet files
        # Remove trailing slash and bulletin_raw duplication
        base_path = export_folder_path.rstrip('/')
        pattern = f"{base_path}/{endpoint}/{partition_date_prefix}*/{partition_time_prefix}*/{parquet_extension}"

        # Find all matching files, sorted by path (newest first due to timestamp format)
        files = sorted(glob.glob(pattern), reverse=True)

        if not files:
            print(
                f"No existing Parquet files found for {endpoint}, using default baseline"
            )
            return default_baseline

        # Read most recent file and get max modified timestamp
        df = pl.read_parquet(files[0])

        if "payload" in df.columns and len(df) > 0:
            # Extract 'modified' field from JSON payload column
            max_modified = df.select(
                pl.col("payload").str.json_path_match("$.modified").alias("modified")
            ).select(pl.col("modified").max()).item()
            print(f"Found baseline for {endpoint}: {max_modified} (from {files[0]})")
            return max_modified

    except Exception as e:
        print(f"Error reading Parquet for {endpoint}: {e}")

    return default_baseline


def cleanup_old_local_files(
    current_file_path: str,
    get_config: ConfigResource,
    keep_versions: int = 2,
    context: AssetExecutionContext = None,
) -> None:
    """
    Remove old local Parquet files, keeping only the N most recent versions per endpoint.

    Only runs after successful S3 upload to ensure data safety. Retains latest versions
    locally for auto-incremental baseline and recovery if S3 is unavailable.

    Args:
        current_file_path (str): Path to the file that was just uploaded.
        get_config (ConfigResource): ConfigResource for getting config values.
        keep_versions (int): Number of recent versions to retain locally (default: 2).
        context (AssetExecutionContext): For logging (optional).
    """
    partition_date_prefix = get_config.get_config_value("partition_date_prefix", required=True)
    partition_time_prefix = get_config.get_config_value("partition_time_prefix", required=True)

    try:
        # Extract endpoint from path: bulletin_raw/{endpoint}/load_date=.../load_time=.../file.parquet
        path_parts = Path(current_file_path).parts

        # Find bulletin_raw index
        if "bulletin_raw" not in path_parts:
            return

        bulletin_idx = path_parts.index("bulletin_raw")
        if bulletin_idx + 1 >= len(path_parts):
            return

        endpoint = path_parts[bulletin_idx + 1]

        # Build base path for this endpoint
        base_path = str(Path(current_file_path).parents[0])
        while partition_time_prefix in base_path:
            base_path = str(Path(base_path).parent)
        while partition_date_prefix in base_path:
            base_path = str(Path(base_path).parent)

        # Find all load_date/load_time directories for this endpoint
        pattern = f"{base_path}/{partition_date_prefix}*/{partition_time_prefix}*"
        dirs = sorted(glob.glob(pattern), reverse=True)  # Newest first

        if len(dirs) <= keep_versions:
            if context:
                context.log.info(
                    f"Retention: {len(dirs)} version(s) found, keeping all (limit: {keep_versions})"
                )
            return

        # Delete older load_time directories
        deleted_count = 0
        for old_dir in dirs[keep_versions:]:
            shutil.rmtree(old_dir)
            deleted_count += 1
            if context:
                context.log.info(f"Cleaned up old local version: {old_dir}")

        # Clean up empty load_date parent directories
        date_pattern = f"{base_path}/{partition_date_prefix}*"
        date_dirs = glob.glob(date_pattern)
        
        for date_dir in date_dirs:
            # Check if directory is empty or only contains empty subdirectories
            try:
                if not any(Path(date_dir).iterdir()):
                    # Directory is completely empty
                    shutil.rmtree(date_dir)
                    if context:
                        context.log.info(f"Removed empty date directory: {date_dir}")
            except (OSError, StopIteration):
                pass

        if context:
            context.log.info(
                f"Local retention policy applied: Kept {keep_versions} latest versions, "
                f"removed {deleted_count} old version(s) for endpoint '{endpoint}'"
            )

    except Exception as e:
        if context:
            context.log.warning(f"Failed to cleanup old local files: {e}")
        else:
            print(f"Warning: Failed to cleanup old local files: {e}")
