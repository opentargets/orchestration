from google.cloud import batch_v1
from pydantic import BaseModel


class LogsSpec(BaseModel):
    """Cloud logging specification."""

    def build(self) -> batch_v1.LogsPolicy:
        """Build a `google.cloud.batch_v1.LogsPolicy` object from the logs specification.

        Returns:
            batch_v1.LogsPolicy: The built LogsPolicy object.


        Example:
        ---
        >>> lp = LogsSpec().build()
        >>> lp.destination == batch_v1.LogsPolicy.Destination.CLOUD_LOGGING
        True
        >>> lp.cloud_logging_option.use_generic_task_monitored_resource
        False
        """
        return batch_v1.LogsPolicy(
            destination=batch_v1.LogsPolicy.Destination.CLOUD_LOGGING,
            cloud_logging_option=batch_v1.LogsPolicy.CloudLoggingOption(
                use_generic_task_monitored_resource=False,
            ),
        )
