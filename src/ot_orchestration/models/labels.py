"""Models for google cloud labels."""

from typing import Literal

from pydantic import BaseModel


class LabelModel(BaseModel):
    """Generic labels describing the DAG resource.

    These labels can represent either VM, Dataproc cluster, google batch job
    that is created in the unified pipeline or gentropy pipelines.
    """

    team: Literal["open-targets"] = "open-targets"
    subteam: Literal["backend", "data", "genetics"]
    environment: Literal["development", "staging", "production"]


class UnifiedPipelineStepLabelModel(LabelModel):
    """Generic labels describing a step (single job) in the unified-pipeline."""

    tool: Literal["ontoform", "pis", "etl-backend", "gentropy", "pos"]
    step_name: str
    product: Literal["ppp", "platform"]
    created_by: Literal["unified-pipeline"]


class GentropyPipelineStepLabelModel(LabelModel):
    """Generic labels describing a step (single job) in the gentropy pipelines."""

    tool: Literal["gentropy"]
    step_name: str
    product: Literal[
        # Finemapping dags
        "gwas_catalog_sumstats_pics",
        "gwas_catalog_sumstats_susie_clumping",
        "gwas_catalog_sumstats_susie_finemapping",
        "gwas_catalog_top_hits",
        "credible_set_qc",
        "ukb_ppp_eur_finemapping",
        # Ingestion dags
        "eqtl_catalogue_ingestion",
        "finngen_ingesion",
        "foldx_ingestion",
        "gnomad_ingestion",
        # Harmonisation dags
        "finngen_ukb_meta_harmonisation",
        "gwas_catalog_harmonisation",
        "ukb_ppp_eur_harmonsiation",
        # Other dags
        "genetics_etl",
        "gwas_curation_update",
    ]
    created_by: Literal["gentropy-pipeline"]
