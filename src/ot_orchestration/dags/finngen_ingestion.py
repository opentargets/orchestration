"""Airflow DAG for the Preprocess part of the pipeline."""

from __future__ import annotations

from pathlib import Path

from airflow.models.baseoperator import chain
from airflow.models.dag import DAG
from airflow.utils.trigger_rule import TriggerRule

from ot_orchestration.utils import common
from ot_orchestration.utils.dataproc import (
    create_cluster,
    delete_cluster,
    install_dependencies,
    submit_step,
)
from ot_orchestration.utils.utils import read_yaml_config

SOURCE_CONFIG_FILE_PATH = Path(__file__).parent / "config" / "finngen_ingestion.yaml"
config = read_yaml_config(SOURCE_CONFIG_FILE_PATH)

with DAG(
    dag_id=Path(__file__).stem,
    description="Open Targets Genetics — Finngen Susie Finemapping Results Ingestion",
    default_args=common.shared_dag_args,
    **common.shared_dag_kwargs,
):
    finngen_finemapping_ingestion = submit_step(
        cluster_name=config["cluster_name"],
        step_id="finngen_finemapping_ingestion",
        task_id="finngen_finemapping_ingestion",
        other_args=[
            f"step.finngen_finemapping_out={config['credible_set_output_path']}",
            f"step.finngen_susie_finemapping_snp_files={config['finngen_snp_input_glob']}",
            f"step.finngen_susie_finemapping_cs_summary_files={config['finngen_credible_set_input_glob']}",
            "step.session.start_hail=true",
            "step.session.write_mode=overwrite",
        ],
        trigger_rule=TriggerRule.ALL_DONE,
    )

    finngen_study_index = submit_step(
        cluster_name=config["cluster_name"],
        step_id="finngen_studies",
        task_id="finngen_studies",
        other_args=[
            f"step.finngen_study_index_out={config['study_index_output_path']}",
            f"step.finngen_phenotype_table_url={config['phenotype_table_url']}",
            f"step.finngen_release_prefix={config['finngen_release_prefix']}",
            f"step.finngen_summary_stats_url_prefix={config['finngen_summary_stats_url_prefix']}",
            f"step.finngen_summary_stats_url_suffix={config['finngen_summary_stats_url_suffix']}",
            "step.session.write_mode=overwrite",
        ],
        trigger_rule=TriggerRule.ALL_DONE,
    )
    chain(
        create_cluster(
            config["cluster_name"],
            autoscaling_policy=config["autoscaling_policy"],
            master_disk_size=2000,
            num_workers=6,
        ),
        install_dependencies(config["cluster_name"]),
        finngen_study_index,
        finngen_finemapping_ingestion,
        delete_cluster(config["cluster_name"]),
    )
