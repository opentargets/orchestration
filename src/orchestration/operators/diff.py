"""Custom operators for the Platform part of the pipeline."""

from collections.abc import Iterable, Sequence

from airflow.operators.branch import BaseBranchOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.utils.context import Context
from google.cloud.storage import Client

from orchestration.dags.config.unified_pipeline import UnifiedPipelineConfig
from orchestration.operators.differs.differ import Differ
from orchestration.utils.common import GCP_PROJECT_PLATFORM


class DiffOperator(BaseBranchOperator):
    """Custom operator that decides whether to run a step or not.

    At the moment, this operator will check the parts of the configuration that
    are relevant to the current step, comparing them against the copy in the
    specified bucket. If they are different, that means the step should be run.

    It would be interesting add a check that downloads the manifest from the
    specified bucket and compares the resources listed for the step with the
    files in the bucket, along with their checksums. That way we can ensure the
    files exist and have not been tampered with.

    If any of the Differs returns True, the operator will branch to a step named
    `{step_name}_diff_yes`, otherwise it will branch to `{step_name}_diff_no`.

    Args:
        project_id: The GCP project ID. Defaults to the platform project.
        step_name: The name of the step to run if needed.
        differs: A list of Differ instances.
        diff_yes_task: The task to branch to if there are differences.
        diff_no_task: The task to branch to if there are no differences.
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
        diff_yes_task: str,
        diff_no_task: str,
        config: UnifiedPipelineConfig,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.project_id = project_id
        self.step_name = step_name
        self.differs = differs
        self.diff_yes_task = diff_yes_task
        self.diff_no_task = diff_no_task
        self.config = config
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain
        self.client: Client

    def choose_branch(self, context: Context) -> str | Iterable[str]:
        """Decide whether to run a step or not."""
        for differ in self.differs:
            differ_name = differ.__class__.__name__
            if not isinstance(differ, Differ):
                raise TypeError("differs must implement is_diff method")

            self.logger().info(f"checking differ {differ_name} for step {self.step_name}")
            if differ.is_diff(step_name=self.step_name, config=self.config, client=self.client):
                self.log.info(f"{differ_name} triggered, step {self.step_name} will run")
                return self.diff_yes_task

        self.log.info("no differences found, step will not run")
        return self.diff_no_task

    def execute(self, context: Context):
        hook = GCSHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )

        self.client = hook.get_conn()

        return self.do_branch(context, self.choose_branch(context))
