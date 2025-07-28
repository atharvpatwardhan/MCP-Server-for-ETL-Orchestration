import boto3
from botocore.exceptions import ClientError
import time

athena_client = boto3.client('athena')

def run_athena_query(query: str, database: str, s3_output_location: str) -> str:
    try:
        response = athena_client.start_query_execution(
            QueryString=query,
            QueryExecutionContext={'Database': database},
            ResultConfiguration={'OutputLocation': s3_output_location}
        )
        return response['QueryExecutionId']
    except ClientError as e:
        return f"Failed to start Athena query: {e.response['Error']['Message']}"

def get_query_status(query_execution_id: str) -> str:
    try:
        response = athena_client.get_query_execution(QueryExecutionId=query_execution_id)
        return response['QueryExecution']['Status']['State']
    except ClientError as e:
        return f"Failed to get query status: {e.response['Error']['Message']}"

def get_query_results(query_execution_id: str, max_results: int = 100) -> list:
    try:
        paginator = athena_client.get_paginator('get_query_results')
        results = []
        for page in paginator.paginate(QueryExecutionId=query_execution_id, MaxResults=max_results):
            for row in page['ResultSet']['Rows']:
                results.append([col.get('VarCharValue', '') for col in row['Data']])
        return results
    except ClientError as e:
        return [f"Failed to get query results: {e.response['Error']['Message']}"]

def list_databases() -> list:
    try:
        response = athena_client.list_databases(CatalogName='AwsDataCatalog')
        return response.get('DatabaseList', [])
    except ClientError as e:
        return [f"Failed to list databases: {e.response['Error']['Message']}"]

def list_tables(database: str) -> list:
    try:
        response = athena_client.list_table_metadata(CatalogName='AwsDataCatalog', DatabaseName=database)
        tables = [table['Name'] for table in response.get('TableMetadataList', [])]
        return tables
    except ClientError as e:
        return [f"Failed to list tables: {e.response['Error']['Message']}"]
