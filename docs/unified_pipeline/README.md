# Unified pipeline dag

The unified pipeline DAG is responsible for orchestrating the generation of an Open Targets Platform data release.


## Guidelines for running a release

1. Create a dedicated feature branch in the `orchestration` repository to capture all run-specific changes.
2. Update `src/orchestration/dags/config/unified_pipeline.yaml` with the required software and data versions. For Open Targets–managed repositories (PIS, PTS, Gentropy, ETL), ensure a release tag covers the latest changes and reference that tag explicitly in the configuration.
3. Provision a fresh Airflow environment in Google Cloud by running `make cloud-dev`.
4. Open <http://localhost:8081/dags/unified_pipeline/grid?tab=graph> and trigger the `unified_pipeline` DAG via the **Trigger DAG** button.
5. Monitor the run and address any task failures. Expect to spend time debugging until the DAG completes successfully.
    I. If the error is related with step configuration, make the necessary changes in your local machine and pull the changes to the cloud instance. Delete the configuration file from the run directory (typically located at `gs://opentargets-pipeline-runs/<run_id>/etc/config`) and re-trigger the failing step recursively (cluster recreation is not necessary).
    II. If the error is related with business logic in PIS, for example, create a new tag with the fix and update the `unified_pipeline.yaml` file with the new tag. Then, re-trigger the failing step recursively tearing down any existing machine.
    III. If the error is related with Cloud infrastructure, orchestrate manually steps until the heavy lifting is done.
6. Execute the metrics suite (see https://github.com/opentargets/ot-release-metrics) to validate the release output.
