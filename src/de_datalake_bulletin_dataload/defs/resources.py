import dagster as dg
from dagster import ConfigurableResource
from contextlib import contextmanager
import os
import datetime
import boto3
import time
import json
from typing import Optional, Dict


class ConfigResource(ConfigurableResource):
    """Resource for loading fetch configuration from JSON file."""
    config_path: str
    
    def load_config(self) -> dict:
        """Load configuration from JSON file."""
        with open(self.config_path, 'r') as f:
            return json.load(f)
    
    def get_endpoints(self) -> Dict[str, str]:
        """Get endpoints dictionary from config."""
        config = self.load_config()
        return config.get('endpoints', {})
    
    def get_endpoint_keys(self) -> list:
        """Get list of all available endpoint keys."""
        return list(self.get_endpoints().keys())
    
    def get_pagination_param(self) -> str:
        """Get pagination parameter from config."""
        config = self.load_config()
        return config.get('pagination', '')
    
    def get_sensor_param(self) -> str:
        """Get sensor parameter from config."""
        config = self.load_config()
        return config.get('sensor_param', '')


class HTTPClientResource(ConfigurableResource):
    """Resource for managing HTTP client connections."""
    user_agent: str
    base_url: str
    get_config: ConfigResource
    
    def get_headers(self):
        """Assign header for HTTP requests."""
        return {"User-Agent": self.user_agent}
    
    def get_all_endpoint_urls(self) -> Dict[str, str]:
        """Get full URLs for all configured endpoints."""
        endpoints = self.get_config.get_endpoints()
        pagination_param = self.get_config.get_pagination_param()
        return {
            key: f"{self.base_url}{path}{pagination_param}"
            for key, path in endpoints.items()
        }
    
    def get_endpoint_keys(self) -> list:
        """Get list of all available endpoint keys."""
        return self.get_config.get_endpoint_keys()
        
class ParquetExportResource(ConfigurableResource):
    """Resource for managing Parquet export file location and settings."""
    export_folder_path: str
    parquet_file_name: str
    compression: str = "SNAPPY"

    def get_export_path(self, endpoint_key: str, load_date: str, load_time: str):
        """Generate export path with optional endpoint suffix in filename to split the final output into files specific to endpoints data was fetched."""
        datestamp = str(load_date)
        timestamp = str(load_time)
        
        if endpoint_key:
            filename = f"{endpoint_key}_{self.parquet_file_name}"
        else:
            filename = self.parquet_file_name
        
        export_path = os.path.join(self.export_folder_path, f"{endpoint_key}/load_date={datestamp}/load_time={timestamp}/{filename}")
        return export_path

class AWSS3Resource(ConfigurableResource):
    """Resource for managing AWS S3 connections."""
    bucket_name: str
    access_key_id: str
    secret_access_key: str
    region_name: str

    def get_s3_client(self):
        s3_client = boto3.client(
            's3',
            region_name=self.region_name,
            aws_access_key_id=self.access_key_id,
            aws_secret_access_key=self.secret_access_key
        )
        return s3_client