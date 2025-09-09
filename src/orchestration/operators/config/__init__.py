"""Config logger operators."""

from airflow.models.baseoperator import BaseOperator

from orchestration.dags.config.staging import StagingPipelineConfig


class StagingPipelineConfigLogOperator(BaseOperator):
    """Logs the configuration used for the DAG run."""

    def __init__(self, config: StagingPipelineConfig, **kwargs) -> None:
        super().__init__(**kwargs)
        self.config = config

    def execute(self, context: dict) -> None:
        """Log the configuration."""
        self.log.info("DAG run configuration:")
        self.log.info(f"Config release date: {self.config.release_date}")
        assert self.config.templated.validated, "Configuration not validated"
        self.log.info(f"Config environment: {self.config.templated.validated.env}")
        self.log.info(f"Config release URI: {self.config.templated.validated.release_uri}")
        self.log.info("Config steps:")
        self.log.info(self.config.templated.validated.steps)
        for step in self.config.templated.validated.steps:
            self.log.info(f" - {step.name}, depends on: {step.depends_on}")
        self.log.info("Config environment specs:")
        for env in self.config.templated.validated.environment_specs:
            self.log.info(f" - {env.name}: {env.vars}")
        self.log.info("End of configuration.")
