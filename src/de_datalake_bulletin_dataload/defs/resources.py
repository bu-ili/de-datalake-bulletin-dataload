from dagster import ConfigurableResource
import os
import boto3
import json
import httpx
import importlib.metadata
from typing import Dict, Optional


class ConfigResource(ConfigurableResource):
    """Resource for loading configuration and managing HTTP client settings.

    Provides centralized configuration management for API requests, including
    endpoint definitions, pagination, sensor parameters, and HTTP client settings.
    User-Agent is dynamically generated from package version.

    Attributes:
        config_path (str): Path to the JSON configuration file.
    """

    config_path: str

    def load_config(self) -> dict:
        """
        Load configuration from JSON file.

        Returns:
            dict: Configuration data loaded from the JSON file.

        Raises:
            FileNotFoundError: If the config file does not exist.
        """
        if not os.path.exists(self.config_path):
            raise FileNotFoundError(f"Config file not found at: {self.config_path}")
        with open(self.config_path, "r") as f:
            return json.load(f)

    def get_endpoint_keys(self) -> list:
        """Get list of all available endpoint keys.

        Returns:
            list: List of endpoint keys.

        Raises:
            ValueError: If 'endpoints' field is missing or empty in config file.
        """
        endpoints = self.get_config_value("endpoints", required=True)
        endpoint_keys = endpoints.keys()
        if not endpoint_keys:
            raise ValueError("No endpoint keys found in config file")
        return list(endpoint_keys)

    def get_user_agent(self) -> str:
        """
        Get user agent string with dynamic version from package metadata.

        Returns:
            str: User agent string in format 'BU-DataEngineering-Bulletin-Loader/{version}'.
        """
        try:
            version = importlib.metadata.version("de_datalake_bulletin_dataload")
        except importlib.metadata.PackageNotFoundError:
            version = "unknown"

        return f"BU-DataEngineering-Bulletin-Loader/{version}"

    def get_headers(self) -> Dict[str, str]:
        """
        Get headers for HTTP requests.

        Returns:
            Dict[str, str]: Headers dictionary with dynamic User-Agent.
        """
        return {"User-Agent": self.get_user_agent()}

    def get_http_client_config(self) -> dict:
        """
        Get httpx client configuration from config file.

        Returns:
            dict: Configuration for httpx.AsyncClient with limits, timeout, and HTTP/2 settings.

        Raises:
            ValueError: If http_client configuration is missing in config file.
        """
        config = self.load_config()
        http_config = config.get("http_client")

        if not http_config:
            raise ValueError("'http_client' configuration is missing in config file")

        return {
            "limits": httpx.Limits(
                max_connections=http_config["max_connections"],
                max_keepalive_connections=http_config["max_keepalive_connections"],
            ),
            "timeout": httpx.Timeout(
                http_config["total_timeout"], connect=http_config["connect_timeout"]
            ),
            "http2": http_config["http2"],
        }

    def get_all_endpoint_urls(self) -> Dict[str, str]:
        """Get full URLs for all configured endpoints.

        Returns:
            Dict[str, str]: Mapping of endpoint keys to full URLs with pagination.
        """
        endpoints = self.get_config_value("endpoints", required=True)
        pagination_param = self.get_config_value("loop_pagination_param", required=True)
        base_url = self.get_config_value("base_url", required=True)
        return {
            key: f"{base_url}{path}{pagination_param}"
            for key, path in endpoints.items()
        }

    def get_config_value(self, key: str, default=None, required=False):
        """Get a value from config by key.

        Args:
            key (str): The config key to retrieve.
            default: Default value if key not found (only used if required=False).
            required (bool): If True, raises ValueError when key is missing or empty.

        Returns:
            The config value.

        Raises:
            ValueError: If required=True and key is missing or value is empty.
        """
        config = self.load_config()
        value = config.get(key, default)

        if required and not value:
            raise ValueError(f"'{key}' is missing or empty in config file")

        return value


class ParquetExportResource(ConfigurableResource):
    """
    Resource for managing Parquet export file location and settings.
    Values can be provided directly or will be read from ConfigResource.

    Attributes:
        config_resource (ConfigResource): Config resource for reading default values.
        export_folder_path (Optional[str]): Base folder path for exporting Parquet files.
        parquet_file_name (Optional[str]): Base name for Parquet files.
        compression (Optional[str]): Compression type for Parquet files (default: from config or SNAPPY).
    """

    config_resource: ConfigResource
    export_folder_path: Optional[str] = None
    parquet_file_name: Optional[str] = None
    compression: Optional[str] = None

    @staticmethod
    def _normalize_path_fragment(value: str) -> str:
        """Normalize one path fragment (single segment)."""
        return str(value).strip().strip("/\\")

    @staticmethod
    def _normalize_path_prefix(value: str) -> str:
        """Normalize a root/prefix path for S3 keys."""
        return str(value).strip().strip("/\\")

    def _get_export_folder_path(self) -> str:
        """Get export folder path from attribute or config."""
        if self.export_folder_path:
            return str(self.export_folder_path).strip()
        paths = self.config_resource.get_config_value("paths", required=True)
        folder_path = paths.get("parquet_export_folder_path")
        if not folder_path:
            raise ValueError("'paths.parquet_export_folder_path' is missing or empty in config file")
        return str(folder_path).strip()

    def _get_parquet_file_name(self) -> str:
        """Get parquet file name from attribute or config."""
        if self.parquet_file_name:
            return self.parquet_file_name
        paths = self.config_resource.get_config_value("paths", required=True)
        file_name = paths.get("parquet_file_name")
        if not file_name:
            raise ValueError("'paths.parquet_file_name' is missing or empty in config file")
        return file_name

    def _get_compression(self) -> str:
        """Get compression type from attribute or config."""
        if self.compression:
            return self.compression
        paths = self.config_resource.get_config_value("paths", required=True)
        compression = paths.get("parquet_compression")
        if not compression:
            raise ValueError("'paths.parquet_compression' is missing or empty in config file")
        return compression

    def get_compression(self) -> str:
        """Public accessor for parquet compression setting."""
        return self._get_compression()

    def _get_partition_date_prefix(self) -> str:
        """Get partition date prefix from config."""
        return self.config_resource.get_config_value("partition_date_prefix", required=True)

    def _get_partition_time_prefix(self) -> str:
        """Get partition time prefix from config."""
        return self.config_resource.get_config_value("partition_time_prefix", required=True)

    def _build_partitioned_filename_path(
        self, endpoint_key: str, load_date: str, load_time: str
    ) -> str:
        """Build endpoint/date/time/file suffix used by both local and S3 paths."""
        endpoint = self._normalize_path_fragment(endpoint_key)
        datestamp = str(load_date).strip()
        timestamp = str(load_time).strip()
        file_name = self._get_parquet_file_name()
        date_prefix = self._get_partition_date_prefix()
        time_prefix = self._get_partition_time_prefix()
        filename = f"{endpoint}_{file_name}" if endpoint else file_name
        return os.path.join(
            endpoint,
            f"{date_prefix}{datestamp}",
            f"{time_prefix}{timestamp}",
            filename,
        )

    def get_export_root_prefix(self) -> str:
        """
        Return the normalized export root prefix (no leading/trailing slash).

        This value is safe to use for both local paths and S3 object key prefixes.
        """
        return self._normalize_path_prefix(self._get_export_folder_path())

    def get_relative_export_path(
        self, endpoint_key: str, load_date: str, load_time: str
    ) -> str:
        """
        Generate a normalized relative export path (without local filesystem root).

        Format:
        {export_root}/{endpoint}/{partition_date_prefix}{YYYY-MM-DD}/{partition_time_prefix}{HH:MM:SS}/{endpoint}_{file_name}
        """
        folder_path = self.get_export_root_prefix()

        return os.path.join(
            folder_path,
            self._build_partitioned_filename_path(
                endpoint_key=endpoint_key, load_date=load_date, load_time=load_time
            ),
        )

    def get_export_path(self, endpoint_key: str, load_date: str, load_time: str):
        """
        Generate export path with endpoint prefix in filename.

        Args:
            endpoint_key (str): The endpoint key for file naming prefix.
            load_date (str): Date string for partitioning (YYYY-MM-DD). Used for creating folder structure for standardized S3 data lake upload.
            load_time (str): Time string for partitioning (HH:MM:SS). Used for creating folder structure for standardized S3 data lake upload.

        Returns:
            str: Full export path for Parquet file.
        """
        folder_path = self._get_export_folder_path()
        return os.path.join(
            folder_path,
            self._build_partitioned_filename_path(
                endpoint_key=endpoint_key, load_date=load_date, load_time=load_time
            ),
        )


class AWSS3Resource(ConfigurableResource):
    """Resource for managing AWS S3 connections for Parquet file uploads.
    Credentials must be provided via environment variables.
    Bucket name and region can be provided directly or read from ConfigResource.

    Attributes:
        config_resource (ConfigResource): Config resource for reading default values.
        access_key_id (str): AWS access key ID (required, from env var).
        secret_access_key (str): AWS secret access key (required, from env var).
        bucket_name (Optional[str]): Name of the S3 bucket (from config if not provided).
        region_name (Optional[str]): AWS region name (from config if not provided).
    """

    config_resource: ConfigResource
    access_key_id: str
    secret_access_key: str
    bucket_name: Optional[str] = None
    region_name: Optional[str] = None

    def _get_bucket_name(self) -> str:
        """Get S3 bucket name from attribute or config."""
        if self.bucket_name:
            return self.bucket_name
        aws_config = self.config_resource.get_config_value("aws", required=True)
        bucket_name = aws_config.get("s3_bucket_name")
        if not bucket_name:
            raise ValueError("'aws.s3_bucket_name' is missing or empty in config file")
        return bucket_name

    def _get_region_name(self) -> str:
        """Get AWS region name from attribute or config."""
        if self.region_name:
            return self.region_name
        aws_config = self.config_resource.get_config_value("aws", required=True)
        region_name = aws_config.get("region_name")
        if not region_name:
            raise ValueError("'aws.region_name' is missing or empty in config file")
        return region_name

    def get_s3_client(self):
        """Get S3 client with lazy initialization and credential validation.

        Returns:
            boto3.client: Configured S3 client instance.

        Raises:
            ValueError: If S3 bucket does not exist or credentials are invalid.
        """
        if not hasattr(self, "_client"):
            bucket = self._get_bucket_name()
            region = self._get_region_name()
            try:
                self._client = boto3.client(
                    "s3",
                    region_name=region,
                    aws_access_key_id=self.access_key_id,
                    aws_secret_access_key=self.secret_access_key,
                )
                self._client.head_bucket(Bucket=bucket)
            except Exception as e:
                error_msg = str(e)
                if "NoSuchBucket" in error_msg:
                    raise ValueError(f"S3 bucket '{bucket}' does not exist")
                else:
                    raise ValueError(f"Failed to connect to S3: {error_msg}")
        return self._client
