from dagster import ConfigurableResource
import os
import boto3
import json
from typing import Optional, Dict


class ConfigResource(ConfigurableResource):
    """
    Resource for loading fetch configuration from JSON file.
    
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
        with open(self.config_path, 'r') as f:
            return json.load(f)
    
    def get_endpoints(self) -> Dict[str, str]:
        """
        Get endpoints dictionary from config.
        
        Returns:
            Dict[str, str]: Endpoints mapping from config file.
            
        Raises:
            ValueError: If 'endpoints' field is missing or empty in config file.
        """
        config = self.load_config()
        endpoints = config.get('endpoints', {})
        if not endpoints:
            raise ValueError("'endpoints' field is missing or empty in config file")
        return endpoints
    
    def get_endpoint_keys(self) -> list:
        """
        Get list of all available endpoint keys.
        
        Returns:
            list: List of endpoint keys.
        
        Raises:
            ValueError: If 'endpoints' field is missing or empty in config file via get_endpoints().
        """
        return list(self.get_endpoints().keys())
    
    def get_pagination_param(self) -> str:
        """
        Get pagination parameter from config that drives page size for API requests.
        
        Returns:
            str: Pagination parameter.
            
        Raises:
            ValueError: If 'pagination' field is missing or empty in config file.
        """
        config = self.load_config()
        pagination = config.get('pagination', '')
        if not pagination:
            raise ValueError("'pagination' field is missing or empty in config file")
        return pagination
    
    def get_sensor_param(self) -> str:
        """
        Get sensor parameter from config that drives data change detection.
        
        Returns:
            str: Sensor parameter for determining if DAG should materialize.

        Raises:
            ValueError: If 'sensor_param' field is missing or empty in config file.
        """
        config = self.load_config()
        return config.get('sensor_param', '')
    
    def get_base_url(self) -> str:
        """
        Get base URL from config file.
        
        Returns:
            str: Base URL for API requests.
            
        Raises:
            ValueError: If base_url is missing or empty in config file.
        """
        config = self.load_config()
        base_url = config.get('base_url', '')
        if not base_url:
            raise ValueError("base_url is missing or empty in config file")
        return base_url


class HTTPClientResource(ConfigurableResource):
    """
    Resource for managing HTTP client connections.
    
    Attributes:
        user_agent (str): User-Agent string for HTTP requests.
        get_config (ConfigResource): ConfigResource for getting base URL and endpoints.
    """
    user_agent: str
    get_config: ConfigResource
    
    def get_headers(self) -> Dict[str, str]:
        """
        Get headers for HTTP requests.
        
        Returns:
            Dict[str, str]: Headers dictionary with User-Agent.
        """
        return {"User-Agent": self.user_agent}
    
    def get_base_url(self) -> str:
        """
        Get base URL from config file.
        
        Returns:
            str: Base URL for API requests.
        """
        return self.get_config.get_base_url()

    def get_all_endpoint_urls(self) -> Dict[str, str]:
        """
        Get full URLs for all configured endpoints.
        
        Returns:
            Dict[str, str]: Mapping of endpoint keys to full URLs with pagination.
        """
        endpoints = self.get_config.get_endpoints()
        pagination_param = self.get_config.get_pagination_param()
        base_url = self.get_base_url()
        return {
            key: f"{base_url}{path}{pagination_param}"
            for key, path in endpoints.items()
        }
    
    def get_endpoint_keys(self) -> list:
        """
        Get list of all available endpoint keys.
        
        Returns:
            list: List of endpoint keys.
        """
        return self.get_config.get_endpoint_keys()
        
class ParquetExportResource(ConfigurableResource):
    """
    Resource for managing Parquet export file location and settings.
    
    Attributes:
        export_folder_path (str): Base folder path for exporting Parquet files.
        parquet_file_name (str): Base name for Parquet files.
        compression (str): Compression type for Parquet files (default: SNAPPY).
    """
    export_folder_path: str
    parquet_file_name: str
    compression: str = "SNAPPY"

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
        
        if endpoint_key:
            filename = f"{endpoint_key}_{self.parquet_file_name}"
        else:
            filename = self.parquet_file_name
        
        export_path = os.path.join(self.export_folder_path, f"{endpoint_key}/load_date={datestamp}/load_time={timestamp}/{filename}")
        return export_path

class AWSS3Resource(ConfigurableResource):
    """Resource for managing AWS S3 connections for Parquet file uploads.
    
    Attributes:
        bucket_name (str): Name of the S3 bucket.
        access_key_id (str): AWS access key ID.
        secret_access_key (str): AWS secret access key.
        region_name (str): AWS region name.
    """
    bucket_name: str
    access_key_id: str
    secret_access_key: str
    region_name: str

    def get_s3_client(self):
        """Get S3 client with lazy initialization and credential validation.
        
        Returns:
            boto3.client: Configured S3 client instance.
            
        Raises:
            ValueError: If S3 bucket does not exist or credentials are invalid.
        """
        if not hasattr(self, '_client'):
            try:
                self._client = boto3.client(
                    's3',
                    region_name=self.region_name,
                    aws_access_key_id=self.access_key_id,
                    aws_secret_access_key=self.secret_access_key
                )
                self._client.head_bucket(Bucket=self.bucket_name)
            except Exception as e:
                error_msg = str(e)
                if "NoSuchBucket" in error_msg:
                    raise ValueError(f"S3 bucket '{self.bucket_name}' does not exist")
                else:
                    raise ValueError(f"Failed to connect to S3: {error_msg}")
        return self._client