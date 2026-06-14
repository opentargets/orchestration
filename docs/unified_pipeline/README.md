# Unified pipeline dag

The unified pipeline DAG orchestrates a dev-scoped pipeline run whose outputs can be reviewed and promoted later.


## Guidelines for running the pipeline

1. Create a dedicated feature branch in the `orchestration` repository to capture all run-specific changes.
2. Update `src/orchestration/dags/config/unified_pipeline.yaml` with the required software and data versions. For Open Targets-managed repositories (PIS, PTS, Gentropy, ETL), reference explicit tags in the configuration so the run is reproducible against fixed code versions. Also set `run_name` to identify this run (for example `sz/platform-2605-1` or `sz/ppp-2605-1`); all DAG outputs are written under `gs://open-targets-pipeline-runs/<run_name>`, and any later promotion to a release location happens outside the DAG. See [config.md](config.md) for the full format.
3. Provision a fresh Airflow environment in Google Cloud by running `make cloud-dev`.
4. Open <http://localhost:8081/dags/unified_pipeline/grid?tab=graph> and trigger the `unified_pipeline` DAG via the **Trigger DAG** button.
5. Monitor the run and address any task failures. Expect to spend time debugging until the DAG completes successfully.
    I. If the error is related with step configuration, make the necessary changes in your local machine and pull the changes to the cloud instance. Delete the configuration file from the run directory (typically located at `gs://open-targets-pipeline-runs/<run_name>/etc/config`) and re-trigger the failing step recursively (cluster recreation is not necessary).
    II. If the error is related with business logic in PIS, for example, create a new tag with the fix and update the `unified_pipeline.yaml` file so the rerun still points to an explicit version. Then, re-trigger the failing step recursively tearing down any existing machine.
    III. If the error is related with Cloud infrastructure, orchestrate manually steps until the heavy lifting is done.
6. Execute the metrics suite (see https://github.com/opentargets/ot-release-metrics) to validate the run output before any later promotion.
