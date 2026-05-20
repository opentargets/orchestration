#!/bin/bash
#########################################################################################
# Using VEP to annotate consequences on canonical transcripts only, within 0.5Mbp of the variant
# to generate the variant index of the Open Targets Platform.
#
# This script is intended to be run on a google batch VM. To run the script,
# use the google batch operator and `annotate_transcripts.sh` script.
#
# Usage:
#   INPUT_FILE=input.vcf \
#   CACHE_DIR=path/to/vep/cache/dir \
#   OUTPUT_FILE=output.json \
#   INPUT_DIR=path/to/input/dir \
#   OUTPUT_DIR=path/to/output/dir \
#   annotate_variants.sh
#
#
# Notes:
# * The input is referenced as stdin in the VEP --input-file `-` option,
#   which allows us to use a pipe to feed the input file to VEP after modifying the header with sed.
#   https://superuser.com/questions/1391610/how-to-reference-stdin-as-an-option-in-a-program-in-a-pipeline
# * The comments are striped when the script is parsed to run on batch
#   to preserve the minimal command.
#
#########################################################################################
set -euo pipefail
# Templated variables (defined in runnable_spec.script_variables)
readonly INPUT_DIR="${input_dir}"
readonly CACHE_DIR="${cache_dir}"
readonly OUTPUT_DIR="${output_dir}"
#########################################################################################
sed '1s/^CHROM/#CHROM/' "${INPUT_DIR}/${INPUT_FILE}" | \
    vep \
    --cache \
    --offline \
    --format vcf \
    --fork 4 \
    --force_overwrite \
    --no_stats \
    --dir_cache "${CACHE_DIR}" \
    --input_file /dev/stdin \
    --output_file "${OUTPUT_DIR}/${OUTPUT_FILE}" \
    --json \
    --dir_plugins "${CACHE_DIR}/VEP_plugins" \
    --sift b \
    --fasta "${CACHE_DIR}/Homo_sapiens.GRCh38.dna.primary_assembly.fa.gz" \
    --mane_select \
    --appris \
    --hgvsg \
    --pick_order mane_select,canonical \
    --per_gene \
    --uniprot \
    --symbol \
    --biotype \
    --check_existing \
    --exclude_null_alleles \
    --canonical \
    --plugin TSSDistance,both_direction=1 \
    --distance 500000 \
    --plugin Conservation,"${CACHE_DIR}/gerp_conservation_scores.homo_sapiens.GRCh38.bw",MAX \
    --plugin LoF,loftee_path:"${CACHE_DIR}/VEP_plugins",gerp_bigwig:"${CACHE_DIR}/gerp_conservation_scores.homo_sapiens.GRCh38.bw",human_ancestor_fa:"${CACHE_DIR}/human_ancestor.fa.gz",conservation_file:/opt/vep/loftee.sql \
    --plugin AlphaMissense,file="${CACHE_DIR}/AlphaMissense_hg38.tsv.gz",transcript_match=1
