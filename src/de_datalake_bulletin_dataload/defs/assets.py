import dagster as dg
from dagster import asset, AssetExecutionContext, Config
import asyncio
import httpx
import json
from pydantic import BaseModel, StrictStr, StrictInt, StrictBool, ValidationError, Field
from typing import List, Any, Dict, Union
import duckdb
from datetime import datetime
import time
import os
from de_datalake_bulletin_dataload.defs.resources import DuckDBResource, HTTPClientResource, ParquetExportResource

class GuidObject(BaseModel):
    rendered: StrictStr

class TitleObject(BaseModel):
    rendered: StrictStr

class ContentObject(BaseModel):
    rendered: StrictStr
    protected: StrictBool

class ExcerptObject(BaseModel):
    rendered: StrictStr
    protected: StrictBool
    
class ExpectedJSONSchema(BaseModel):
    """Definition of the expected JSON schema from the WordPress Pages API, with strict type enforcement."""
    id: StrictInt
    date: StrictStr
    date_gmt: StrictStr
    guid: GuidObject
    modified: StrictStr
    modified_gmt: StrictStr
    slug: StrictStr
    status: StrictStr
    type: StrictStr
    link: StrictStr
    title: TitleObject
    content: ContentObject
    excerpt: ExcerptObject
    author: StrictInt
    featured_media: StrictInt
    parent: StrictInt
    menu_order: StrictInt
    comment_status: StrictStr
    ping_status: StrictStr
    template: StrictStr
    meta: Union[Dict[str, Any], list] = Field(default_factory=dict)
    links: Dict[str, Any] = Field(alias="_links", default_factory=dict)

    class Config:
        extra = "forbid" 
        populate_by_name = True

class Violation(BaseModel):
    """Definition to store notification of schema violation."""
    kind: str
    field: str
    message: str

class SchemaViolationError(Exception):
    """Custom exception to represent schema violations."""
    def __init__(self, id, violation: Violation, message: str = None):
        self.id = id
        self.violation = violation
        self.message = message
        super().__init__(self._format())

    def _format(self):
        lines = [f"Schema violation for id={self.id}:"]
        for d in self.violation:
            lines.append(f"- [{d.kind}] {d.field}: {d.message}")
        return "\n".join(lines)


async def fetch_page(session: httpx.AsyncClient, page_number: int,  total_pages: int,  base_url: str, headers: dict, context: AssetExecutionContext) -> list:
    """Fetch a single page of data from the API asynchronously."""
    try:
        context.log.info(f"Fetching page {page_number} of {total_pages}")
        response = await session.get(url=f"{base_url}{page_number}", headers=headers)
        page_data = response.json()
        context.log.info(f"Page {page_number} fetched with {len(page_data)} records.")
        return page_data
    except Exception as e:
        context.log.error(f"Error fetching page {page_number}: {e}")
        return []


async def fetch_all_pages(base_url: str, headers: dict, context: AssetExecutionContext) -> list:
    """Fetch all pages concurrently from the API."""
    limits = httpx.Limits(max_connections=30)
    timeout = httpx.Timeout(30.0)
    async with httpx.AsyncClient(limits=limits, timeout=timeout) as session:
        response = await session.get(url=base_url+"1", headers=headers)
        total_pages = int(response.headers.get("X-WP-TotalPages", 1))
        first_page_data = response.json()
        
        context.log.info(f"Total pages to fetch: {total_pages}")
        
        tasks = [
            fetch_page(session, page_num, total_pages, base_url, headers, context) 
            for page_num in range(1, total_pages + 1)
        ]
        
        all_pages = await asyncio.gather(*tasks)
        
        all_data = []
        for page_data in all_pages:
            all_data.extend(page_data)
        
        return all_data

def validate_single_response(data: dict, context: AssetExecutionContext) -> ExpectedJSONSchema:
    """
    Validate a single WordPress API response against the expected schema.
    
    :param data: Dictionary containing WordPress page data
    :return: Validated ExpectedJSONSchema instance
    :raises SchemaViolationError: If validation fails
    """
    violations = []
    response_id = str(data.get('id', '<missing_id>'))

    try:
        return ExpectedJSONSchema(**data)
    except ValidationError as e:
        for error in e.errors():
            field_path = ".".join(str(loc) for loc in error['loc'])
            violations.append(Violation(
                kind=error['type'],
                field=field_path,
                message=error['msg']
            ))
        raise SchemaViolationError(id=response_id, violation=violations, message="Schema validation failed")

def validate_batch_responses(data: List[dict], context: AssetExecutionContext) -> List[ExpectedJSONSchema]:
    """
    Validate multiple WordPress API responses.
    
    :param data_list: List of dictionaries containing WordPress page data
    :return: List of validated ExpectedJSONSchema instances
    """
    validated_records = []
    for item in data:
        validated_records.append(validate_single_response(item, context))
    return validated_records

def insert_data_to_duckdb(data: list, duckdb_database: str, context: AssetExecutionContext) -> int:
    """Insert fetched data into DuckDB table using batch operations."""
    duckdb_conn = duckdb.connect(database=duckdb_database, read_only=False)
    context.log.info("Established connection with DuckDB. Checking if table exists and preparing to insert data...")

    duckdb_conn.execute("""CREATE TABLE IF NOT EXISTS de_datalake_bulletin_data (
        id INTEGER,
        modified TIMESTAMP,
        payload JSON
    );""")

    latest_date = duckdb_conn.execute("SELECT MAX(modified) FROM de_datalake_bulletin_data").fetchone()[0]
    if latest_date is None:
        latest_date = datetime.min
        context.log.info(f"No existing data found in DuckDB. Setting latest_date to {latest_date}.")
    else:
        context.log.info(f"Latest date in DuckDB: {latest_date}")
    
    data_to_insert = [item for item in data if datetime.fromisoformat(item.get("modified")) > latest_date]

    batch_data = []
    for item in data_to_insert:
        payload = json.dumps(item)
        id = item.get("id")
        modified = item.get("modified")
        batch_data.append((id, modified, payload))

    duckdb_conn.executemany(
        "INSERT INTO de_datalake_bulletin_data (id, modified, payload) VALUES (?, ?, ?)",
        batch_data
    )
    context.log.info(f"Inserted {len(batch_data)} records into DuckDB successfully.")
    
    duckdb_conn.close()
    return len(batch_data)

def export_data_to_parquet(parquet_file_path: str, duckdb_database: str, context: AssetExecutionContext) -> str:
    """Export data from DuckDB to Parquet file."""
    duckdb_conn = duckdb.connect(database=duckdb_database, read_only=False)

    if not os.path.exists(parquet_file_path):
        context.log.warning(f"Parquet file at {parquet_file_path} does not exist. Exporting all data.")
        latest_modified_date = datetime.min

    else:
        context.log.info(f"Parquet file at {parquet_file_path} exists. Exporting only new/updated data.")
        latest_modified = duckdb.sql(f"""SELECT CAST(MAX(modified) AS TIMESTAMP) AS latest_modified_date FROM read_parquet('{parquet_file_path}')""").df().iloc[0,0]
        latest_modified_str = str(latest_modified)
        latest_modified_date = datetime.fromisoformat(latest_modified_str)
        context.log.info(f"Latest date of data available in Parquet: {latest_modified_date}")
    
    duckdb_conn.execute(f"""COPY
                 (SELECT id, modified, payload from de_datalake_bulletin_data where modified > '{latest_modified_date}') 
                 TO '{parquet_file_path}' 
                 (FORMAT PARQUET)""")
    
    duckdb_conn.close()
    context.log.info(f"Data exported to Parquet at {parquet_file_path}.")
    return parquet_file_path


@asset(
    group_name="de_datalake_bulletin_dataload",
    description="Raw BU Bulletin data fetched from WordPress Pages API."
)
async def fetch_bulletin_data(context: AssetExecutionContext, http_client: HTTPClientResource) -> list:
    """Fetch all bulletin data from the API asynchronously."""
    start_time = time.perf_counter()
    context.log.info("Starting bulletin data fetch from API")
    
    headers = http_client.get_headers()
    data = await fetch_all_pages(http_client.base_url, headers, context)
    
    elapsed_time = time.perf_counter() - start_time
    context.log.info(f"Fetched {len(data)} records in {elapsed_time:.2f} seconds")
    
    return data

@asset(
    group_name="de_datalake_bulletin_dataload",
    description="Validating raw BU Bulletin data against expected JSON schema"
)
def validate_bulletin_data(context: AssetExecutionContext, fetch_bulletin_data: list) -> list:
    """Validate raw bulletin data against the expected schema."""
    start_time = time.perf_counter()
    context.log.info("Starting data validation")
    
    validated_data = validate_batch_responses(fetch_bulletin_data, context)
    
    # Convert Pydantic models back to dictionaries for downstream processing
    validated_responses = [item.model_dump(by_alias=True) for item in validated_data]
    
    elapsed_time = time.perf_counter() - start_time
    context.log.info(f"Validated {len(validated_responses)} records in {elapsed_time:.2f} seconds")
    
    return validated_responses

@asset(
    group_name="de_datalake_bulletin_dataload",
    description="Bulletin data stored in DuckDB table"
)
def insert_bulletin_data_to_duckdb(context: AssetExecutionContext,  duckdb_connection: DuckDBResource, validate_bulletin_data: list) -> int:
    """Insert raw bulletin data into DuckDB table."""
    start_time = time.perf_counter()
    context.log.info("Starting DuckDB insertion")
    
    records_inserted = insert_data_to_duckdb(validate_bulletin_data, duckdb_connection.database, context)
    
    elapsed_time = time.perf_counter() - start_time
    context.log.info(f"Inserted {records_inserted} records into DuckDB in {elapsed_time:.2f} seconds")
    
    return records_inserted

@asset(
    group_name="de_datalake_bulletin_dataload",
    description="Bulletin data exported to Parquet file"
)
def export_bulletin_data_to_parquet(context: AssetExecutionContext, parquet_export_path: ParquetExportResource, duckdb_connection: DuckDBResource, insert_bulletin_data_to_duckdb: int) -> str:
    """Export bulletin data from DuckDB to Parquet file."""
    start_time = time.perf_counter()
    context.log.info("Starting Parquet export")
    
    parquet_path = export_data_to_parquet(parquet_export_path.get_export_path(), duckdb_connection.database, context)
    
    elapsed_time = time.perf_counter() - start_time
    context.log.info(f"Exported data to Parquet in {elapsed_time:.2f} seconds")
    context.log.info(f"Parquet file location: {parquet_path}")
    
    return parquet_path