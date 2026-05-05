#!/bin/bash
#########################################################################################
# Annotate transcripts using VEP. This script is intended to be run on a google batch VM.
# To run the script, use the google batch operator.
#
# Usage:
#   INPUT_FILE=path/to/input.vcf \
#   CACHE_DIR=path/to/vep/cache/dir \
#   OUTPUT_FILE=path/to/output.json \
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

[[ -f "${INPUT_FILE}" ]]  || { echo "INPUT_FILE not found: ${INPUT_FILE}"; exit 1; }
[[ -d "${CACHE_DIR}" ]]   || { echo "CACHE_DIR not found: ${CACHE_DIR}"; exit 1; }
[[ -d "${OUTPUT_FILE%/*}" ]] || { echo "OUTPUT_FILE parent directory not found: ${OUTPUT_FILE%/*}"; exit 1; }

sed '1s/^CHROM/#CHROM/' "${INPUT_FILE}" | \
    vep \
    --cache \
    --offline \
    --format vcf \
    --fork 4 \
    --force_overwrite \
    --no_stats \
    --dir_cache "${CACHE_DIR}" \
    --input_file - \
    --output_file "${OUTPUT_FILE}" \
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
