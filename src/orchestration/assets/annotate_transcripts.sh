#!/bin/bash
#########################################################################################
# Annotate transcripts using VEP. This script is intended to be run on a google batch VM.
# To run the script, use the google batch operator and `annotate_transcripts.sh` script.
#
# Usage:
#   INPUT_FILE=path/to/input.vcf \
#   CACHE_DIR=path/to/vep/cache/dir \
#   OUTPUT_FILE=path/to/output.json \
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

sed '1s/^CHROM/#CHROM/' "${INPUT_FILE}" | \
    vep \
    --cache \
    --offline \
    --format vcf \
    --fork 4 \
    --force_overwrite \
    --no_stats \
    --dir_cache ${CACHE_DIR} \
    --input_file - \
    --output_file ${OUTPUT_FILE} \
    --json \
    --mane \
    --appris \
    --hgvsg \
    --fasta ${CACHE_DIR}/Homo_sapiens.GRCh38.dna.primary_assembly.fa.gz \
    --uniprot \
    --symbol \
    --biotype \
    --protein \
    --canonical \
    --plugin TSSDistance,both_direction=1