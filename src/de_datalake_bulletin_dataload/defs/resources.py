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
        endpoints = self.get_config_value("endpoints", default={}, required=True)
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
            "http2": http_config.get("http2", False),
        }

    def get_all_endpoint_urls(self) -> Dict[str, str]:
        """Get full URLs for all configured endpoints.

        Returns:
            Dict[str, str]: Mapping of endpoint keys to full URLs with pagination.
        """
        endpoints = self.get_config_value("endpoints", default={}, required=True)
        pagination_param = self.get_config_value(
            "loop_pagination_param", default="", required=True
        )
        base_url = self.get_config_value("base_url", default="", required=True)
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

    def _get_export_folder_path(self) -> str:
        """Get export folder path from attribute or config."""
        if self.export_folder_path:
            return self.export_folder_path
        return self.config_resource.get_config_value(
            "paths", default={}, required=True
        ).get("parquet_export_folder_path", "/bulletin_raw/")

    def _get_parquet_file_name(self) -> str:
        """Get parquet file name from attribute or config."""
        if self.parquet_file_name:
            return self.parquet_file_name
        return self.config_resource.get_config_value(
            "paths", default={}, required=True
        ).get("parquet_file_name", "de_bulletin_data.parquet")

    def _get_compression(self) -> str:
        """Get compression type from attribute or config."""
        if self.compression:
            return self.compression
        return self.config_resource.get_config_value(
            "paths", default={}, required=True
        ).get("parquet_compression", "SNAPPY")

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
        datestamp = str(load_date)
        timestamp = str(load_time)
        
        folder_path = self._get_export_folder_path()
        file_name = self._get_parquet_file_name()

        if endpoint_key:
            filename = f"{endpoint_key}_{file_name}"
        else:
            filename = file_name

        export_path = os.path.join(
            folder_path,
            f"{endpoint_key}/load_date={datestamp}/load_time={timestamp}/{filename}",
        )
        return export_path


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
        return self.config_resource.get_config_value(
            "aws", default={}, required=True
        ).get("s3_bucket_name", "")

    def _get_region_name(self) -> str:
        """Get AWS region name from attribute or config."""
        if self.region_name:
            return self.region_name
        return self.config_resource.get_config_value(
            "aws", default={}, required=True
        ).get("region_name", "us-east-1")

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
