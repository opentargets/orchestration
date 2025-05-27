import logging

from google.cloud.storage import Client

from orchestration.dags.config.unified_pipeline import UnifiedPipelineConfig


class SparkJobDiffer:
    """Check whether the spark job has finished successfully.

    This class checks that the output folder of a step contains the _SUCCESS file.

    Args:
        outputs (dict[str, str] | None): The output map of the step. The keys are
            the output names and the values are the output URIs. If None, it will
            use the output map from the step-specific config.
    """

    def __init__(self, outputs: dict[str, str] | None = None):
        self.outputs = outputs
        self.logger = logging.getLogger(__name__)

    def is_diff(self, *, step_name: str, config: UnifiedPipelineConfig, client: Client) -> bool:
        """Ensure _SUCCESS file exists in the output folder.

        Args:
            step_name (str): The name of the step to compare.
            config (UnifiedPipelineConfig): The unified pipeline configuration.
            client (Client): The Google Cloud Storage client used in the differ.

        Returns:
            bool: Whether the job has finished successfully.
        """
        if not self.outputs:
            self.outputs = config.step_specific_config(step_name).get("output", [])
        for output in self.outputs.values():
            # output can be a dict with a path key (ETL) or a strng (Gentropy)
            if isinstance(output, dict):
                output_uri = str(output.get("path"))
            else:
                output_uri = str(output)
            self.logger.info(f"checking step {step_name} output {output_uri}")
            bucket, prefix = output_uri.removeprefix("gs://").split("/", 1)
            blob_name = f"{prefix}/_SUCCESS"
            b = client.bucket(bucket).blob(blob_name)
            if not b.exists():
                self.logger.info(f"step output {output_uri} is missing _SUCCESS file")
                return True
            self.logger.info(f"step output {step_name} has _SUCCESS file")

        # if we reach this, all outputs are correct
        return False
