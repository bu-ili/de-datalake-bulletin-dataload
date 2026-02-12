from dagster import AssetExecutionContext
from datetime import datetime
import os
import json
import polars as pl
from de_datalake_bulletin_dataload.defs.resources import (
    ParquetExportResource,
    AWSS3Resource,
)


def export_to_parquet(
    export_path: ParquetExportResource,
    validated_data: list,
    endpoint_key: str,
    load_date: str,
    load_time: str,
    context: AssetExecutionContext,
) -> str:
    """
    Export data to Parquet format using Polars.

    Creates a Parquet file with structure: id, dl_inserted_at, payload, dl_hash.

    Args:
        export_path (ParquetExportResource): Dagster resource configuration for Parquet export.
        validated_data (list): List of validated data dictionaries to be exported.
        endpoint_key (str): The endpoint key for file naming.
        load_date (str): Date string for partitioning (YYYY-MM-DD).
        load_time (str): Time string for partitioning (HH:MM:SS).
        context (AssetExecutionContext): Dagster AssetExecutionContext for logging.

    Returns:
        str: Path to the exported Parquet file.

    Raises:
        Exception: If any error occurs during file writing.
    """
    export_file_path = export_path.get_export_path(
        endpoint_key=endpoint_key, load_date=load_date, load_time=load_time
    )
    runtime_timestamp = datetime.now()

    os.makedirs(os.path.dirname(export_file_path), exist_ok=True)
    context.log.info(
        f"Creating an export file at {export_file_path} for endpoint {endpoint_key}."
    )

    export_data = [
        {
            "id": data["id"],
            "dl_inserted_at": runtime_timestamp,
            "payload": json.dumps(data),
        }
        for data in validated_data
    ]

    export_df = pl.DataFrame(export_data)

    hash_expr = pl.concat_str(
        [
            pl.col(c).cast(pl.Utf8, strict=False).fill_null("NULL")
            for c in export_df.columns
        ],
        separator="|",
    )

    export_df = export_df.with_columns([hash_expr.str.encode("hex").alias("dl_hash")])

    export_df.write_parquet(export_file_path, compression="snappy")
    context.log.info(
        f"Data exported to Parquet at {export_file_path}. Total rows: {len(export_df)}"
    )

    return export_file_path


def export_to_s3(
    aws_s3_config: AWSS3Resource, file_path: str, context: AssetExecutionContext
) -> str:
    """
    Upload a file to an AWS S3 bucket, preserving the subfolder structure.

    Args:
        aws_s3_config (AWSS3Resource): Dagster resource configuration for AWS S3.
        file_path (str): Path to the file to be uploaded.
        context (AssetExecutionContext): Dagster AssetExecutionContext for logging.

    Returns:
        str: S3 URI of the uploaded file.

    Raises:
        Exception: If any error occurs during the upload process.
    """
    s3_client = aws_s3_config.get_s3_client()
    bucket_name = aws_s3_config._get_bucket_name()

    s3_key = file_path.replace("\\", "/").lstrip("./")

    try:
        file_size = os.path.getsize(file_path)
        context.log.info(f"Uploading {file_path} ({file_size / (1024 * 1024):.2f} MB) to S3...")

        s3_client.upload_file(file_path, bucket_name, s3_key)

        s3_uri = f"s3://{bucket_name}/{s3_key}"
        context.log.info(f"File uploaded to {s3_uri}")

        return s3_uri
    except Exception as e:
        context.log.error(f"Failed to upload {file_path} to S3: {str(e)}")
        raise
