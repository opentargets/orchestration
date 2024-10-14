# Orchestration documentation

This catalog describes how the orchestration works in the current state

### How to generate dag svg files

1. Locate your global `airflow.cfg` file and update the [core] dag_folder in `airflow.cfg` to point to the `src` directory of the orchestration repository or set the `AIRFLOW__CORE__DAGS_FOLDER` environment variable.

2. Run

   ```bash
   poetry run airflow  dags show  --save docs/${DAG_NAME}.svg ${DAG_NAME}
   ```
