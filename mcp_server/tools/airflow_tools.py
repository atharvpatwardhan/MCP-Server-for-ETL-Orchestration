import requests
from tools.airflow_config import AIRFLOW_API_BASE, AIRFLOW_USERNAME, AIRFLOW_PASSWORD
import datetime


def run_airflow_dag(dag_id: str) -> str:
    """Trigger a DAG run in Airflow"""
    url = f"{AIRFLOW_API_BASE}/dags/{dag_id}/dagRuns"
    payload = {
        "conf": {},
        "dag_run_id": f"manual__{datetime.utcnow().isoformat()}"
    }

    response = requests.post(
        url,
        json=payload,
        auth=(AIRFLOW_USERNAME, AIRFLOW_PASSWORD)
    )

    if response.status_code == 200:
        return f"DAG '{dag_id}' triggered successfully."
    else:
        return f"Failed to trigger DAG '{dag_id}': {response.text}"

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


def list_dags_with_status():
    """List DAGs with their paused state and latest run status."""
    url = f"{AIRFLOW_API_BASE}/dags?limit=100"  # Adjust limit as needed
    response = requests.get(url, auth=(AIRFLOW_USERNAME, AIRFLOW_PASSWORD))

    if response.status_code != 200:
        return f"Failed to fetch DAGs: {response.text}"

    dags = response.json().get("dags", [])
    dag_list = []

    for dag in dags:
        dag_id = dag["dag_id"]
        is_paused = dag["is_paused"]

        # Fetch latest run status for this DAG
        status_msg = check_airflow_status(dag_id)
        # You could parse status_msg to just get the state or keep the full message

        dag_list.append({
            "dag_id": dag_id,
            "is_paused": is_paused,
            "latest_run_status": status_msg
        })

    return dag_list
