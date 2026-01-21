from sys import exception
import dagster as dg
from dagster import ConfigurableResource
from contextlib import contextmanager
import duckdb
import httpx
import sqlalchemy
import os


class DuckDBResource(ConfigurableResource):
    """Resource for managing DuckDB database connections."""
    database: str

    @contextmanager
    def get_connection(self):
        conn = duckdb.connect(database=self.database, read_only=False)
        try:
            yield conn
        finally:
            conn.close()


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
    export_path: str
    compression: str = "SNAPPY"

    def get_export_path(self):
        return self.export_path