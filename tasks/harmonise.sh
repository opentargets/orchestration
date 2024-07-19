#!/usr/bin/env bash

# Run the batch harmonisation task
# the input to the harmonisation is the manifest json blob
# {
#   "studyId": "GCST90428603",
#   "rawPath": "/test_batch/GCST90428603/raw/GCST90428603.h.tsv.gz",
#   "isCurated": true,
#   "passHarmonisation": "/test_batch/GCST90428603/harmonised/result.parquet",
#   "manifestPath": "/test_batch/GCST90428603/manifest.json",
#   "isHarmonised": false
# }
# the manifest blob as above needs to be available for each task in the process
#
# harmonisation statuses:
# passHarmonisation: false -> failed harmonisation, needs to be reprocessed
# passHarmonisation: true -> successful harmonisation
# passHarmonisation: null -> not yet processed


gsutil cp $MANIFEST_PATH manifest.json
echo $MANIFEST_PATH
export RAW_PATH=$(jq -r '.rawPath' manifest.json);
export HARMONISED_PATH=$(jq -r '.harmonisedPath' manifest.json);
export IS_HARMONISED=$(jq -r ".isHarmonised" manifest.json);

if [ $IS_HARMONISED = "true" ]; then
    # should not happen, because the task should not be triggered if the data is already harmonised
    echo "Already harmonised $RAW_PATH to $HARMONISED_PATH"
    exit 0
fi
echo "Harmonising $RAW_PATH to $HARMONISED_PATH"
poetry run gentropy step=gwas_catalog_sumstat_preprocess step.raw_sumstats_path=$RAW_PATH step.out_sumstats_path=$HARMONISED_PATH;

LAST_COMMAND_OUTPUT=$?
if [[ $LAST_COMMAND_OUTPUT -eq 0 ]]; then
    echo $(jq '.isHarmonised=true' manifest.json) > manifest.json
else
    echo $(jq '.isHarmonised=false' manifest.json) > manifest.json
fi
echo "Harmonisation completed with status $LAST_COMMAND_OUTPUT"


export MANIFEST_PATH=$(cat manifest.json | jq -r '.manifestPath')
echo "Dumping manifest.json to $MANIFEST_PATH"
cat manifest.json

# only save the manifest.json file if it is not a test run
if [ -z $TEST_RUN ]; then
    gsutil cp manifest.json $MANIFEST_PATH
fi
