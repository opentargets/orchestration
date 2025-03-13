"""Custom operators for Open Targets."""

from airflow.models.baseoperator import BaseOperator


class FTPToGCSOperator(BaseOperator):
    """FTP to Google Cloud Storage operator."""

    def __init__(self, name: str, **kwargs) -> None:
        super().__init__(**kwargs)
        self.name = name

    def execute(self, context):
        """Execute the Operator."""
        message = f"Hello {self.name}"
        print(message)
        return message
