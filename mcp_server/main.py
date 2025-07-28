from mcp.server.fastmcp import FastMCP
from tools.run_airflow_dag import run_airflow_dag
from tools.check_airflow_status import check_airflow_status

mcp = FastMCP("ETL Orchestration")

@mcp.tool()
def run_airflow_dag_tool(dag_id: str) -> str:
    return run_airflow_dag(dag_id)

@mcp.tool()
def check_airflow_status_tool(dag_id: str) -> str:
    return check_airflow_status(dag_id)

if __name__ == "__main__":
    mcp.run()
