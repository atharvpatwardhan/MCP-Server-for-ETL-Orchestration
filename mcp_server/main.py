from mcp.server.fastmcp import FastMCP
from tools.run_airflow_dag import run_airflow_dag
from tools.check_airflow_status import check_airflow_status
from tools.list_dags import list_dags_with_status

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

if __name__ == "__main__":
    mcp.run()
