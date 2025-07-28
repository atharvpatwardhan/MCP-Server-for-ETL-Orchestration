from mcp.server.fastmcp import FastMCP
from tools.s3_tools import create_bucket, upload_file, delete_bucket
from tools.airflow_tools import check_airflow_status, list_dags_with_status, run_airflow_dag
from tools.glue_tools import run_glue_job, get_glue_job_status, list_glue_jobs, get_glue_job_logs

from dotenv import load_dotenv

load_dotenv()

mcp = FastMCP("ETL Orchestration")

@mcp.tool()
def run_airflow_dag_tool(dag_id: str) -> str:
    return run_airflow_dag(dag_id)

@mcp.tool()
def check_airflow_status_tool(dag_id: str) -> str:
    return check_airflow_status(dag_id)

@mcp.tool()
def list_dags_with_status_tool() -> list:
    return list_dags_with_status()

@mcp.tool()
def create_s3_bucket_tool(bucket_name: str, region: str = None) -> str:
    return create_bucket(bucket_name, region)

@mcp.tool()
def upload_s3_file_tool(bucket_name: str, object_key: str, file_path: str) -> str:
    return upload_file(bucket_name, object_key, file_path)

@mcp.tool()
def delete_s3_bucket_tool(bucket_name: str, delete_objects: bool = False) -> str:
    return delete_bucket(bucket_name, delete_objects)

@mcp.tool()
def run_glue_job_tool(job_name: str, arguments: dict = None) -> str:
    return run_glue_job(job_name, arguments)

@mcp.tool()
def get_glue_job_status_tool(job_name: str, job_run_id: str) -> str:
    return get_glue_job_status(job_name, job_run_id)

@mcp.tool()
def list_glue_jobs_tool() -> list:
    return list_glue_jobs()

@mcp.tool()
def get_glue_job_logs_tool(job_run_id: str) -> list:
    return get_glue_job_logs(job_run_id)


if __name__ == "__main__":
    mcp.run()
