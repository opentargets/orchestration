#!/bin/bash
#########################################################################################
# Using VEP to annotate protein altering consequences on all overlapping transcripts for variants
# in the OpenTargets Platform.
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
#   annotate_transcripts.sh
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
    --dir_cache ${CACHE_DIR} \
    --input_file /dev/stdin \
    --output_file ${OUTPUT_DIR}/${OUTPUT_FILE} \
    --json \
    --mane \
    --appris \
    --fasta ${CACHE_DIR}/Homo_sapiens.GRCh38.dna.primary_assembly.fa.gz \
    --uniprot \
    --protein \
    --distance 0 \
    --canonical
