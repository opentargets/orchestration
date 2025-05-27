"""Custom operators for the Platform part of the pipeline."""

from collections.abc import Iterable, Sequence

from airflow.models.taskinstance import TaskInstance
from airflow.operators.branch import BaseBranchOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.utils.context import Context
from google.cloud.storage import Client

from orchestration.dags.config.unified_pipeline import UnifiedPipelineConfig
from orchestration.operators.differs.differ import Differ
from orchestration.utils.common import GCP_PROJECT_PLATFORM


class DiffOperator(BaseBranchOperator):
    """Custom operator that decides whether to run a step or not.

    This operator will run a series of _Differs_ (See
    orchestration.operators.differs.differ.Differ) to determine if a step should
    run. If any differ returns True, the step will run.

    The operator also checks if any upstream step has run. If so, the step will
    run and the differ checks will be skipped. This is because if a step upstream
    of the current one has run, it is likely that the input data for the current
    step has changed.

    If the operator decides the step must run, it will branch to the task specified
    in the `diff_yes_task` parameter. If the step should not run, it will branch to
    the task specified in the `diff_no_task` parameter.

    Args:
        project_id (str): The GCP project ID. Defaults to the platform project.
        step_name (str): The name of the step to run if needed.
        differs (list[Differ]): A list of Differ instances.
        diff_yes_task (str): The task to branch to if the step should run. The
            default value is generated from the step name. E.g. for "etl_go",
            the default value is "etl_stage.etl_go.upload_config_etl_go".
        diff_no_task: The task to branch to if the step should not run. The
            default value is generated from the step name. E.g. for "etl_go",
            the default value is "etl_stage.etl_go.end_etl_go".
        config: The full unified pipeline config used in diff computations.
        gcp_conn_id: The connection ID to use when connecting to Google Cloud.
        impersonation_chain: Optional service account or chain to impersonate.
    """

    template_fields: Sequence[str] = ("project_id", "step_name")

    def __init__(
        self,
        *args,
        project_id: str = GCP_PROJECT_PLATFORM,
        step_name: str,
        differs: list[Differ],
        diff_yes_task: str | None = None,
        diff_no_task: str | None = None,
        config: UnifiedPipelineConfig,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.project_id = project_id
        self.stage_name = step_name.split("_", 1)[0]
        self.step_name = step_name
        self.differs = differs
        self.diff_yes_task = diff_yes_task or f"{self.stage_name}_stage.{step_name}.upload_config_{step_name}"
        self.diff_no_task = diff_no_task or f"{self.stage_name}_stage.{step_name}.end_{step_name}"
        self.config = config
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain
        self.client: Client

    def choose_branch(self, context: Context) -> str | Iterable[str]:
        """Decide whether to run a step or not."""
        task_instance: TaskInstance = context.get("task_instance")
        if not task_instance:
            raise ValueError("task_instance not found in context")

        # check if any upstream step has run, if so, we must run this step
        steps_that_ran = task_instance.xcom_pull(key="steps_that_ran") or []
        steps_upstream = self.config.step_definition(self.step_name).get("depends_on", [])
        self.logger().info(f"checking if any of the upstream steps ({steps_upstream}) have run: {steps_that_ran}")
        if any(step in steps_that_ran for step in steps_upstream):
            self.logger().info(f"upstream step {steps_upstream} has run, forcing run of {self.step_name}")
            # add this step to the xcom, as we check only if the immediately upstream steps ran
            task_instance.xcom_push(key="steps_that_ran", value=[*steps_that_ran, self.step_name])
            return self.diff_yes_task

        for differ in self.differs:
            differ_name = differ.__class__.__name__
            if not isinstance(differ, Differ):
                raise TypeError("differs must implement is_diff method")

            self.logger().info(f"checking differ {differ_name} for step {self.step_name}")
            if differ.is_diff(step_name=self.step_name, config=self.config, client=self.client):
                # push the step name to an xcom for downstream tasks
                task_instance.xcom_push(key="steps_that_ran", value=[*steps_that_ran, self.step_name])
                self.logger().info(f"{differ_name} triggered, step {self.step_name} will run")
                return self.diff_yes_task

        self.logger().info("no differences found, step will not run")
        return self.diff_no_task

    def execute(self, context: Context):
        hook = GCSHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )

        self.client = hook.get_conn()

        return self.do_branch(context, self.choose_branch(context))
