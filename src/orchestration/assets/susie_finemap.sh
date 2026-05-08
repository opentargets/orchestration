#!/bin/bash
#########################################################################################
# Run SuSiE fine-mapping for a single locus using gentropy. This script is intended to
# be run on a Google Batch VM, where each task receives a different LOCUS_INDEX via the
# Batch environment, while STUDY_INDEX_PATH and STUDY_LOCUS_MANIFEST_PATH are shared
# across all tasks in the job.
#
# Usage:
#   LOCUS_INDEX=0 \
#   susie_finemap.sh
#
#########################################################################################

set -euo pipefail

gentropy \
    step=susie_finemapping \
    step.study_index_path={{study_index_path}} \
    step.study_locus_manifest_path={{study_locus_manifest_path}} \
    step.study_locus_index={{locus_index}} \
    step.max_causal_snps=10 \
    step.lead_pval_threshold=1e-5 \
    step.purity_mean_r2_threshold=0.25 \
    step.purity_min_r2_threshold=0.25 \
    step.cs_lbf_thr=2 \
    step.sum_pips=0.95 \
    step.susie_est_tausq=False \
    step.run_carma=False \
    step.run_sumstat_imputation=False \
    step.carma_time_limit=600 \
    step.imputed_r2_threshold=0.9 \
    step.ld_score_threshold=5 \
    step.carma_tau=0.15 \
    step.ld_min_r2=0.8 \
    "+step.session.extended_spark_conf={spark.jars:https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop3-latest.jar}" \
    "+step.session.extended_spark_conf={spark.dynamicAllocation.enabled:false}" \
    "+step.session.extended_spark_conf={spark.driver.memory:30g}" \
    "+step.session.extended_spark_conf={spark.kryoserializer.buffer.max:500m}" \
    "+step.session.extended_spark_conf={spark.driver.maxResultSize:5g}" \
    step.session.write_mode=overwrite \
    step.session.output_partitions=1
