import boto3
from botocore.exceptions import ClientError

glue_client = boto3.client('glue')
logs_client = boto3.client('logs')

def run_glue_job(job_name: str, arguments: dict = None) -> str:
    try:
        args = arguments if arguments else {}
        response = glue_client.start_job_run(JobName=job_name, Arguments=args)
        job_run_id = response['JobRunId']
        return f"Glue job '{job_name}' started with Run ID: {job_run_id}"
    except ClientError as e:
        return f"Failed to start Glue job '{job_name}': {e.response['Error']['Message']}"

def get_glue_job_status(job_name: str, job_run_id: str) -> str:
    try:
        response = glue_client.get_job_run(JobName=job_name, RunId=job_run_id)
        state = response['JobRun']['JobRunState']
        return f"Glue job '{job_name}' Run ID '{job_run_id}' status: {state}"
    except ClientError as e:
        return f"Failed to get status: {e.response['Error']['Message']}"

def list_glue_jobs() -> list:
    jobs = []
    try:
        paginator = glue_client.get_paginator('get_jobs')
        for page in paginator.paginate():
            jobs.extend([job['Name'] for job in page['Jobs']])
        return jobs
    except ClientError as e:
        return [f"Failed to list Glue jobs: {e.response['Error']['Message']}"]

def get_glue_job_logs(job_run_id: str, log_group='/aws-glue/jobs/output', limit=50) -> list:
    try:
        streams = logs_client.describe_log_streams(
            logGroupName=log_group,
            orderBy='LastEventTime',
            descending=True,
            limit=5
        )

        messages = []
        for stream in streams['logStreams']:
            events = logs_client.get_log_events(
                logGroupName=log_group,
                logStreamName=stream['logStreamName'],
                limit=limit,
                startFromHead=False
            )
            for event in events['events']:
                messages.append(event['message'])
        return messages
    except Exception as e:
        return [f"Failed to fetch logs: {str(e)}"]
