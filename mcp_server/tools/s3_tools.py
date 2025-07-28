import boto3
from botocore.exceptions import ClientError

# Initialize the S3 client
s3_client = boto3.client('s3')

def create_bucket(bucket_name: str, region: str = None) -> str:
    try:
        create_bucket_configuration = {'LocationConstraint': region} if region else {}
        if region:
            s3_client.create_bucket(
                Bucket=bucket_name,
                CreateBucketConfiguration=create_bucket_configuration
            )
        else:
            s3_client.create_bucket(Bucket=bucket_name)
        return f"Bucket '{bucket_name}' created successfully."
    except ClientError as e:
        return f"Failed to create bucket '{bucket_name}': {e.response['Error']['Message']}"

def upload_file(bucket_name: str, object_key: str, file_path: str) -> str:
    try:
        s3_client.upload_file(file_path, bucket_name, object_key)
        return f"File '{file_path}' uploaded to '{bucket_name}/{object_key}'."
    except ClientError as e:
        return f"Failed to upload file: {e.response['Error']['Message']}"

def delete_bucket(bucket_name: str, delete_objects: bool = False) -> str:
    try:
        if delete_objects:
            # Delete all objects first
            response = s3_client.list_objects_v2(Bucket=bucket_name)
            if 'Contents' in response:
                objects_to_delete = [{'Key': obj['Key']} for obj in response['Contents']]
                s3_client.delete_objects(Bucket=bucket_name, Delete={'Objects': objects_to_delete})
        s3_client.delete_bucket(Bucket=bucket_name)
        return f"Bucket '{bucket_name}' deleted successfully."
    except ClientError as e:
        return f"Failed to delete bucket '{bucket_name}': {e.response['Error']['Message']}"
