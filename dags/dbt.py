from airflow.sdk import dag
from airflow.providers.standard.operators.bash import BashOperator
from pathlib import Path
import json
from receipts import stg_receipts

PATH_TO_DBT_PROJECT = Path(__file__).parent.parent / "dbt/airbnb"
PATH_TO_DBT_VENV = Path(__file__).parent.parent / ".venv/bin/activate"
DBT_MANIFEST_PATH = Path(PATH_TO_DBT_PROJECT) / "target" / "manifest.json"


@dag(dag_id="dbt_dynamic_dag_from_manifest",
     schedule=stg_receipts
     )
def dbt_dag():
    with open(DBT_MANIFEST_PATH) as f:
        manifest = json.load(f)

    dbt_tasks = {}

    # 2. Create a task for each dbt model
    for node_id, node_info in manifest["nodes"].items():
        if node_info["resource_type"] == "model":
            model_name = node_info["name"]

            # Create a BashOperator to run the specific model
            dbt_tasks[node_id] = BashOperator(
                task_id=f"dbt_run_{model_name}",
                bash_command=(
                    f"dbt run --select {model_name} "
                    f"--project-dir {PATH_TO_DBT_PROJECT} "
                    f"--profiles-dir {PATH_TO_DBT_PROJECT} "
                    "--no-write-json "
                    "--target dev"
                ),
            )

    # 3. Set up dependencies between the tasks
    for node_id, node_info in manifest["nodes"].items():
        if node_info["resource_type"] == "model":
            # Get the upstream dependencies for the current model
            upstream_nodes = node_info.get("depends_on", {}).get("nodes", [])

            for upstream_node_id in upstream_nodes:
                # Check if the upstream node is also a model in our tasks
                if upstream_node_id in dbt_tasks:
                    # Create the dependency link
                    dbt_tasks[upstream_node_id] >> dbt_tasks[node_id]


dbt_dag()
