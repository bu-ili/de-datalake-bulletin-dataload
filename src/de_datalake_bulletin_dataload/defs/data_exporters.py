import dagster as dg
from dagster import asset, AssetExecutionContext
from datetime import datetime
import os
import polars as pl
from de_datalake_bulletin_dataload.defs.resources import ParquetExportResource, AWSS3Resource


def export_to_parquet(export_path: ParquetExportResource, validated_data: list, context: AssetExecutionContext) -> str:
    """
    Export data to Parquet format using Polars.

    Arguments:
        parquet_config: Dagster resource configuration for Parquet export
        validated_data: List of validated data to be exported
        context: Dagster AssetExecutionContext for logging purposes
    
    Returns:
        str: Path to the exported Parquet file
    
    Raises:
        Exception: If any error occurs during database operations or file writing
    """
    export_file_path = export_path.get_export_path()
    
    # Create parent directories if they don't exist
    os.makedirs(os.path.dirname(export_file_path), exist_ok=True)
    
    if not os.path.exists(export_file_path):
        context.log.warning(f"Parquet file at {export_file_path} does not exist. Exporting all data.")
    else:
        context.log.info(f"Parquet file at {export_file_path} exists. Overwriting the file.")
            
    df = pl.DataFrame(validated_data)
            
    df.write_parquet(export_file_path, compression="snappy")
    context.log.info(f"Data exported to Parquet at {export_file_path}. Total rows: {len(df)}")
        
    return export_file_path


def export_to_s3(aws_s3_config: AWSS3Resource, file_path: str, context: AssetExecutionContext) -> str:
    """
    Upload a file to an AWS S3 bucket, preserving the subfolder structure.

    Arguments:
        aws_s3_config: Dagster resource configuration for AWS S3
        file_path: Path to the file to be uploaded
        context: Dagster AssetExecutionContext for logging purposes
    
    Returns:
        str: S3 URI of the uploaded file
    
    Raises:
        Exception: If any error occurs during the upload process
    """
    s3_client = aws_s3_config.get_s3_client()
    bucket_name = aws_s3_config.bucket_name
    
    # Normalize path to use forward slashes and remove leading ./
    s3_key = file_path.replace("\\", "/").lstrip("./")

    try:
        s3_client.upload_file(file_path, bucket_name, s3_key)
        s3_uri = f"s3://{bucket_name}/{s3_key}"
        context.log.info(f"File {file_path} uploaded to S3 bucket {bucket_name} as {s3_key}.")
        return s3_uri
    except Exception as e:
        context.log.error(f"Failed to upload {file_path} to S3: {str(e)}")
        raise