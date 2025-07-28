import requests
from tools.airflow_config import AIRFLOW_API_BASE, AIRFLOW_USERNAME, AIRFLOW_PASSWORD


def check_airflow_status(dag_id: str) -> str:
    """Check the latest DAG run status"""
    url = f"{AIRFLOW_API_BASE}/dags/{dag_id}/dagRuns?order_by=-execution_date&limit=1"

    response = requests.get(
        url,
        auth=(AIRFLOW_USERNAME, AIRFLOW_PASSWORD)
    )

    if response.status_code == 200:
        runs = response.json().get("dag_runs", [])
        if not runs:
            return f"No DAG runs found for '{dag_id}'."
        
        latest_run = runs[0]
        state = latest_run.get("state", "unknown")
        run_id = latest_run.get("dag_run_id", "")
        return f"Latest run of DAG '{dag_id}' (Run ID: {run_id}) has status: {state}."
    else:
        return f"Failed to fetch DAG status: {response.text}"
