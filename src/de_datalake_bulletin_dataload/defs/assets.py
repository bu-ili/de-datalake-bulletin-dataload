import dagster as dg
from dagster import asset, AssetExecutionContext, Config
import asyncio
import httpx
from datetime import datetime
import time
import os
import polars as pl
from de_datalake_bulletin_dataload.defs.resources import HTTPClientResource, ParquetExportResource, AWSS3Resource
from de_datalake_bulletin_dataload.defs.validators import validate_batch_responses
from de_datalake_bulletin_dataload.defs.data_exporters import export_to_parquet , export_to_s3


async def fetch_page(session: httpx.AsyncClient, page_number: int,  total_pages: int,  base_url: str, headers: dict, context: AssetExecutionContext) -> list:
    """
    Fetch a single page of data from the API asynchronously. This task is called in fetch_all_pages function to fetch data concurrently.
    
    Arguments:
        session: httpx AsyncClient session
        page_number: Page number to fetch
        total_pages: Total number of pages to fetch
        base_url: Base URL of the API
        headers: Headers to include in the request, optional
        context: Dagster AssetExecutionContext for logging purposes

    Returns
        list: API response data for the requested page

    Raises:
        Exception: If any error occurs during the HTTP request
    """
    try:
        context.log.info(f"Fetching page {page_number} of {total_pages}")
        response = await session.get(url=f"{base_url}{page_number}", headers=headers)
        page_data = response.json()
        context.log.info(f"Page {page_number} fetched with {len(page_data)} records.")
        return page_data
    except Exception as e:
        context.log.error(f"Error fetching page {page_number}: {e}")
        return []


async def fetch_all_pages(http_client: HTTPClientResource, context: AssetExecutionContext) -> list:
    """
    Fetch all pages concurrently from the API.
    
    Arguments:
        http_client: HTTPClientResource containing base URL and headers configuration
        context: Dagster AssetExecutionContext for logging purposes
    
    Returns:
        list: API response data fetched from the Wordpress Pages API for all available pages
    
    Raises:
        Exception: If any error occurs during the HTTP request
    """
    limits = httpx.Limits(max_connections=30)
    timeout = httpx.Timeout(30.0)
    async with httpx.AsyncClient(limits=limits, timeout=timeout) as session:
        response = await session.get(url=http_client.base_url+"1", headers=http_client.get_headers())
        total_pages = int(response.headers.get("X-WP-TotalPages", 1))
        first_page_data = response.json()
        
        context.log.info(f"Total pages to fetch: {total_pages}")
        
        tasks = [
            fetch_page(session, page_num, total_pages, http_client.base_url, http_client.get_headers(), context) 
            for page_num in range(1, total_pages + 1)
        ]
        
        all_pages = await asyncio.gather(*tasks)
        
        all_data = []
        for page_data in all_pages:
            all_data.extend(page_data)
        
        return all_data


@asset(
    group_name="de_datalake_bulletin_dataload",
    description="Fetch data from the BU Bulleting Worpress API asynchronously, validate the schema, and export to Parquet."
)
async def fetch_export_pages_data(context: AssetExecutionContext, http_client: HTTPClientResource, parquet_export_path: ParquetExportResource, aws_s3_config: AWSS3Resource) -> str:
    """Fetch data from the BU Bulleting Worpress API asynchronously."""
    start_time = time.perf_counter()
    context.log.info("Starting bulletin data fetch from Wordpress API")
    
    data = await fetch_all_pages(http_client, context)
    time_to_fetch = time.perf_counter() - start_time
    context.log.info(f"Fetched {len(data)} records in {time_to_fetch:.2f} seconds")

    validated_response = validate_batch_responses(data, context)
    validated_data = [item.model_dump(by_alias=True) for item in validated_response]
    time_to_validate = time.perf_counter() - time_to_fetch - start_time
    context.log.info(f"Validated {len(validated_data)} records in {time_to_validate:.2f} seconds")

    export_path = export_to_parquet(export_path=parquet_export_path, validated_data=validated_data, context=context)
    time_to_export = time.perf_counter() - time_to_validate - time_to_fetch - start_time
    context.log.info(f"Exported data to Parquet in {time_to_export:.2f} seconds")

    aws_s3_path = export_to_s3(aws_s3_config=aws_s3_config, file_path=export_path, context=context)
    time_to_upload = time.perf_counter() - time_to_export - time_to_validate - time_to_fetch - start_time
    context.log.info(f"Uploaded data to S3 in {time_to_upload:.2f} seconds")
    
    return aws_s3_path


# def insert_data_to_duckdb(data: list, duckdb_database: str, context: AssetExecutionContext) -> int:
#     """Insert fetched data into DuckDB table using batch operations."""
#     duckdb_conn = duckdb.connect(database=duckdb_database, read_only=False)
#     context.log.info("Established connection with DuckDB. Checking if table exists and preparing to insert data...")

#     duckdb_conn.execute("""CREATE TABLE IF NOT EXISTS de_datalake_bulletin_data (
#         id INTEGER,
#         modified TIMESTAMP,
#         payload JSON
#     );""")

#     latest_date = duckdb_conn.execute("SELECT MAX(modified) FROM de_datalake_bulletin_data").fetchone()[0]
#     if latest_date is None:
#         latest_date = datetime.min
#         context.log.info(f"No existing data found in DuckDB. Setting latest_date to {latest_date}.")
#     else:
#         context.log.info(f"Latest date in DuckDB: {latest_date}")
    
#     data_to_insert = [item for item in data if datetime.fromisoformat(item.get("modified")) > latest_date]

#     batch_data = []
#     for item in data_to_insert:
#         payload = json.dumps(item)
#         id = item.get("id")
#         modified = item.get("modified")
#         batch_data.append((id, modified, payload))

#     duckdb_conn.executemany(
#         "INSERT INTO de_datalake_bulletin_data (id, modified, payload) VALUES (?, ?, ?)",
#         batch_data
#     )
#     context.log.info(f"Inserted {len(batch_data)} records into DuckDB successfully.")
    
#     duckdb_conn.close()
#     return len(batch_data)

# def export_data_to_parquet_duckdb(parquet_file_path: str, duckdb_database: str, context: AssetExecutionContext) -> str:
#     """Export data from DuckDB to Parquet file."""
#     duckdb_conn = duckdb.connect(database=duckdb_database, read_only=False)

#     if not os.path.exists(parquet_file_path):
#         context.log.warning(f"Parquet file at {parquet_file_path} does not exist. Exporting all data.")
#         latest_modified_date = datetime.min

#     else:
#         context.log.info(f"Parquet file at {parquet_file_path} exists. Exporting only new/updated data.")
#         latest_modified = duckdb.sql(f"""SELECT CAST(MAX(modified) AS TIMESTAMP) AS latest_modified_date FROM read_parquet('{parquet_file_path}')""").df().iloc[0,0]
#         latest_modified_str = str(latest_modified)
#         latest_modified_date = datetime.fromisoformat(latest_modified_str)
#         context.log.info(f"Latest date of data available in Parquet: {latest_modified_date}")
    
#     duckdb_conn.execute(f"""COPY
#                  (SELECT id, modified, payload from de_datalake_bulletin_data where modified > '{latest_modified_date}') 
#                  TO '{parquet_file_path}' 
#                  (FORMAT PARQUET)""")
    
#     duckdb_conn.close()
#     context.log.info(f"Data exported to Parquet at {parquet_file_path}.")
#     return parquet_file_path
#     elapsed_time = time.perf_counter() - start_time
#     context.log.info(f"Inserted {records_inserted} records into DuckDB in {elapsed_time:.2f} seconds")
    
#     return records_inserted

# @asset(
#     group_name="de_datalake_bulletin_dataload",
#     description="Bulletin data exported to Parquet file"
# )
# def export_bulletin_data_to_parquet(context: AssetExecutionContext, parquet_export_path: ParquetExportResource, duckdb_connection: DuckDBResource, insert_bulletin_data_to_duckdb: int) -> str:
#     """Export bulletin data from DuckDB to Parquet file."""
#     start_time = time.perf_counter()
#     context.log.info("Starting Parquet export")
    
#     parquet_path = export_data_to_parquet(parquet_export_path.get_export_path(), duckdb_connection.database, context)
    
#     elapsed_time = time.perf_counter() - start_time
#     context.log.info(f"Exported data to Parquet in {elapsed_time:.2f} seconds")
#     context.log.info(f"Parquet file location: {parquet_path}")
    

# @asset(
#     group_name="de_datalake_bulletin_dataload",
#     description="Bulletin data stored in DuckDB table"
# )
# def insert_bulletin_data_to_duckdb(context: AssetExecutionContext,  duckdb_connection: DuckDBResource, validate_bulletin_data: list) -> int:
#     """Insert raw bulletin data into DuckDB table."""
#     start_time = time.perf_counter()
#     context.log.info("Starting DuckDB insertion")
    
#     records_inserted = insert_data_to_duckdb(validate_bulletin_data, duckdb_connection.database, context)
    
#     return parquet_path

# def insert_data_to_postgres(data: list, postgres_connection: PostgresResource, context: AssetExecutionContext) -> int:
#     """
#     Insert fetched data into PostgreSQL table using batch operations. Leveraging existing data to create idempotent inserts based on 'modified' timestamp.
    
#     Arguments:
#         data: List of dictionaries containing validated WordPress pages data
#         postgres_connection: PostgresResource for managing PostgreSQL connections
#         context: Dagster AssetExecutionContext for logging purposes

#     Returns:
#         int: Number of records inserted into PostgreSQL
    
#     Raises:
#         Exception: If any error occurs during database operations
#     """
#     with postgres_connection.get_connection() as conn:
#         context.log.info("Established connection with PostgreSQL. Checking if table exists and preparing to insert data...")
        
#         # Check if the table exists, create if not
#         table_check_query = sqlalchemy.text("""
#             SELECT table_name FROM information_schema.tables 
#             WHERE table_schema = 'de_datalake' AND table_name = 'pages_data';
#         """)
#         table_exists = conn.execute(table_check_query).scalar()
        
#         if not table_exists:
#             context.log.info("Table 'de_datalake.pages_data' does not exist. Creating table...")
#             create_table_query = sqlalchemy.text("""
#                 CREATE TABLE de_datalake.pages_data (
#                     dl_id INTEGER GENERATED BY DEFAULT AS IDENTITY,
#                     id INTEGER,
#                     modified TIMESTAMP,
#                     payload JSONB
#                 );
#             """)
#             conn.execute(create_table_query)
#             conn.commit()
#             context.log.info("Table 'de_datalake.pages_data' created successfully.")
        
#         # Get the latest modified date from existing data
#         latest_date_query = sqlalchemy.text("SELECT MAX(modified) as latest_change_date FROM de_datalake.pages_data;")
#         result = conn.execute(latest_date_query)
#         latest_date = result.scalar()
#         if latest_date is None:
#             latest_date = datetime.min
#             context.log.info(f"No existing data found in PostgreSQL. Setting latest_date to {latest_date}.")
#         else:
#             context.log.info(f"Latest date in PostgreSQL: {latest_date}")
        
#         data_to_insert = [item for item in data if datetime.fromisoformat(item.get("modified")) > latest_date]

#         batch_data = [
#             {
#                 "id": item.get("id"),
#                 "modified": item.get("modified"),
#                 "payload": json.dumps(item)
#             }
#             for item in data_to_insert
#         ]

#         if batch_data:
#             insert_query = sqlalchemy.text("""
#                 INSERT INTO de_datalake.pages_data (id, modified, payload) 
#                 VALUES (:id, :modified, :payload);
#             """)
#             conn.execute(insert_query, batch_data)
#             conn.commit()
#             context.log.info(f"Inserted {len(batch_data)} records into PostgreSQL successfully.")
#         else:
#             context.log.info("No new records to insert.")
        
#         return len(batch_data)

# def export_data_to_parquet_postgres(parquet_file_path: str, postgres_connection: PostgresResource, context: AssetExecutionContext) -> str:
#     """
#     Export data from PostgreSQL to Parquet file using Polars. Leveraging existing data to create incremental exports based on 'modified' timestamp.

#     Arguments:
#         parquet_file_path: File path to export the Parquet file
#         postgres_connection: PostgresResource for managing PostgreSQL connections
#         context: Dagster AssetExecutionContext for logging purposes
    
#     Returns:
#         str: Path to the exported Parquet file
    
#     Raises:
#         Exception: If any error occurs during database operations or file writing
#     """
#     with postgres_connection.get_connection() as conn:
#         if not os.path.exists(parquet_file_path):
#             context.log.warning(f"Parquet file at {parquet_file_path} does not exist. Exporting all data.")
#             latest_modified_date = datetime.min
#         else:
#             context.log.info(f"Parquet file at {parquet_file_path} exists. Exporting only new/updated data.")
            
#             existing_df = pl.read_parquet(parquet_file_path)
#             if len(existing_df) > 0:
#                 latest_modified_date = existing_df['modified'].max()
#                 context.log.info(f"Latest date of data available in Parquet: {latest_modified_date}")
#             else:
#                 latest_modified_date = datetime.min

#         query = sqlalchemy.text("""SELECT 
#                         id
#                         , modified
#                         , payload 
#                     FROM de_datalake.pages_data
#                     WHERE modified > :latest_modified_date""")
        
#         df = pl.read_database(query, connection=conn, execute_options={"parameters": {"latest_modified_date": latest_modified_date}})
        
#         if len(df) > 0:
#             if os.path.exists(parquet_file_path):
#                 existing_df = pl.read_parquet(parquet_file_path)
#                 df = pl.concat([existing_df, df])
            
#             df.write_parquet(parquet_file_path, compression="snappy")
#             context.log.info(f"Data exported to Parquet at {parquet_file_path}. Total rows: {len(df)}")
#         else:
#             context.log.info("No new data to export.")
        
#         return parquet_file_path


# @asset(
#     group_name="de_datalake_bulletin_dataload",
#     description="Validate fetched BU bulletin data against the expected schema."
# )
# def validate_bulletin_data(context: AssetExecutionContext, fetch_bulletin_data: list) -> list:
#     """Validate fetched BU bulletin data against the expected schema."""
#     start_time = time.perf_counter()
#     context.log.info("Starting data validation")
    
#     validated_data = validate_batch_responses(fetch_bulletin_data, context)
    
#     # Convert Pydantic models back to dictionaries for downstream processing
#     validated_responses = [item.model_dump(by_alias=True) for item in validated_data]
    
#     elapsed_time = time.perf_counter() - start_time
#     context.log.info(f"Validated {len(validated_responses)} records in {elapsed_time:.2f} seconds")
    
#     return validated_responses

# @asset(
#     group_name="de_datalake_bulletin_dataload",
#     description="Insert validated bulletin data into PostgreSQL table."
# )
# def insert_bulletin_data_to_postgres(context: AssetExecutionContext,  postgres_connection: PostgresResource, validate_bulletin_data: list) -> int:
#     """Insert validated bulletin data into PostgreSQL table."""
#     start_time = time.perf_counter()
#     context.log.info("Starting PostgreSQL insertion")
    
#     records_inserted = insert_data_to_postgres(validate_bulletin_data, postgres_connection, context)
    
#     elapsed_time = time.perf_counter() - start_time
#     context.log.info(f"Inserted {records_inserted} records into PostgreSQL in {elapsed_time:.2f} seconds")
    
#     return records_inserted

# @asset(
#     group_name="de_datalake_bulletin_dataload",
#     description="Export bulletin data from PostgreSQL to a Parquet file."
# )
# def export_bulletin_data_to_parquet_postgres(context: AssetExecutionContext, parquet_export_path: ParquetExportResource, postgres_connection: PostgresResource, insert_bulletin_data_to_postgres: int) -> str:
#     """Export bulletin data from PostgreSQL to a Parquet file."""
#     start_time = time.perf_counter()
#     context.log.info("Starting Parquet export")
    
#     parquet_path = export_data_to_parquet_postgres(parquet_export_path.get_export_path(), postgres_connection, context)
    
#     elapsed_time = time.perf_counter() - start_time
#     context.log.info(f"Exported data to Parquet in {elapsed_time:.2f} seconds")
#     context.log.info(f"Parquet file location: {parquet_path}")
    
#     return parquet_path
