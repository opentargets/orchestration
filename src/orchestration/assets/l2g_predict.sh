#!/bin/bash
#########################################################################################
# Run the locus-to-gene prediction step of the L2G pipeline using gentropy. This script is intended to
# be run on a Google Batch VM, where each task receives different INPUT_PARTITION and OUTPUT_PARTITION via the Batch environment, while release_uri and l2g_training_version are shared across all tasks in the job.
#
# Usage:
#   INPUT_PARTITION=gs://bucket/input_partition \
#   OUTPUT_PARTITION=gs://bucket/output_partition \
#   l2g_training_version=v1.0.0 \
#   release_uri=gs://bucket/release_uri \
#   l2g_predict.sh
#
#########################################################################################
set -euo pipefail
# Templated variables (to be replaced by by the manifest generator)
readonly L2G_TRAINING_VERSION=${l2g_training_version}
readonly RELEASE_URI=${release_uri}
#########################################################################################
gentropy \
    step=locus_to_gene \
    step.session.write_mode=overwrite \
    step.session.output_partitions=1 \
    step.run_mode="predict" \
    step.l2g_threshold=0.05 \
    step.download_from_hub=true \
    step.explain_predictions=true \
    step.hf_hub_repo_id="opentargets/locus_to_gene_${L2G_TRAINING_VERSION}" \
    step.credible_set_path="$INPUT_PARTITION" \
    step.feature_matrix_path="${RELEASE_URI}/intermediate/l2g_feature_matrix" \
    "+step.session.extended_spark_conf={spark.jars:https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop3-latest.jar}" \
    step.predictions_path="$OUTPUT_PARTITION" \

