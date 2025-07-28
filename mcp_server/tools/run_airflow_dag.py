import requests
from datetime import datetime
from tools.airflow_config import AIRFLOW_API_BASE, AIRFLOW_USERNAME, AIRFLOW_PASSWORD


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
