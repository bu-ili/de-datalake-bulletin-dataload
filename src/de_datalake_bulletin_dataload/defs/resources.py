import dagster as dg
from dagster import ConfigurableResource
from contextlib import contextmanager
import os
import datetime
import boto3
import time



class PostgresResource(ConfigurableResource):
    """Resource for managing PostgreSQL database connections."""
    host: str
    port: int
    user: str
    password: str
    database: str

    @contextmanager
    def get_connection(self):
        conn = sqlalchemy.create_engine(
            f"postgresql+psycopg2://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"
        ).connect()
        try:
            yield conn
        finally:
            conn.close()
    

class HTTPClientResource(ConfigurableResource):
    """Resource for managing HTTP client connections."""
    user_agent: str
    base_url: str
    
    def get_headers(self):
        return {"User-Agent": self.user_agent}


class ParquetExportResource(ConfigurableResource):
    """Resource for managing Parquet export file location and settings."""
    export_folder_path: str
    parquet_file_name: str
    compression: str = "SNAPPY"

    def get_export_path(self):
        datestamp = datetime.datetime.now().strftime("%Y-%m-%d")
        timestamp = datetime.datetime.now().strftime("%H:%M:%S")
        export_path = os.path.join(self.export_folder_path, f"load_date={datestamp}/load_time={timestamp}/{self.parquet_file_name}")
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