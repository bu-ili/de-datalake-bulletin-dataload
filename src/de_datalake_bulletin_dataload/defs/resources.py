import dagster as dg
from dagster import ConfigurableResource
from contextlib import contextmanager
import duckdb
import httpx
import os

class DuckDBResource(ConfigurableResource):
    """Resource for managing DuckDB connections."""
    database: str
    
    @contextmanager
    def get_connection(self):
        conn = duckdb.connect(database=self.database, read_only=False)
        try:
            yield conn
        finally:
            conn.close()

class HTTPClientResource(ConfigurableResource):
    """Resource for managing HTTP client connections."""
    user_agent: str
    base_url: str
    max_connections: int = 25
    timeout: float = 30.0
    
    def get_headers(self):
        return {"User-Agent": self.user_agent}


class ParquetExportResource(ConfigurableResource):
    """Resource for managing Parquet export file location and settings."""
    export_path: str
    compression: str = "SNAPPY"

    def get_export_path(self):
        return self.export_path