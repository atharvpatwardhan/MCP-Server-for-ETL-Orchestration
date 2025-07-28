import requests
from tools.airflow_config import AIRFLOW_API_BASE, AIRFLOW_USERNAME, AIRFLOW_PASSWORD
from tools.check_airflow_status import check_airflow_status

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
