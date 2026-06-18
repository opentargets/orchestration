#!/usr/bin/env bash
# =============================================================================
# Setup test environment for the FinnGen two-way (FinnGen × UKBB)
# meta-analysis DAG (finngen_ukb_meta).
#
# Copies the full manifest and 3 representative summary statistics files from
# the public FinnGen R12 bucket into the test bucket, mirroring the source
# directory structure so the DAG config paths work unchanged (swap the bucket).
#
# Source: gs://finngen-public-data-r12/meta_analysis/ukbb/
# Target: gs://ot_orchestration/test/finngen_meta/meta_analysis/ukbb/
#
# Usage:
#   bash tests/integration_env_setup/setup_test_env_two_way.sh
#   bash tests/integration_env_setup/setup_test_env_two_way.sh --dry-run
# =============================================================================
set -euo pipefail

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
SRC_BASE="gs://finngen-public-data-r12/meta_analysis/ukbb"
DST_BASE="gs://ot_orchestration/test/finngen_meta/meta_analysis/ukbb"

MANIFEST_FILE="finngen_R12_meta_analysis_mapping_with_definitions.tsv"

# FinnGen phenotype codes to copy (fg_phenotype column in manifest)
PHENOTYPES=(
  "T2D"            # Type 2 diabetes
  "K11_CD_STRICT2" # Crohn's disease (strict)
  "K11_UC_STRICT2" # Ulcerative colitis (strict)
)

DRY_RUN=false
if [[ "${1:-}" == "--dry-run" ]]; then
  DRY_RUN=true
  echo "[dry-run] No files will be copied."
fi

gcs_cp() {
  if $DRY_RUN; then
    echo "[dry-run] gcloud storage cp $*"
  else
    gcloud storage cp "$@"
  fi
}

# ---------------------------------------------------------------------------
# Manifest
# ---------------------------------------------------------------------------
echo "==> Copying manifest..."
gcs_cp \
  "${SRC_BASE}/${MANIFEST_FILE}" \
  "${DST_BASE}/${MANIFEST_FILE}"

# ---------------------------------------------------------------------------
# Summary statistics
# ---------------------------------------------------------------------------
echo "==> Copying summary statistics for ${#PHENOTYPES[@]} phenotypes..."
for PHENOTYPE in "${PHENOTYPES[@]}"; do
  SRC_FILE="${SRC_BASE}/summary_stats/finngen_R12_${PHENOTYPE}_meta_out.tsv.gz"
  DST_FILE="${DST_BASE}/summary_stats/finngen_R12_${PHENOTYPE}_meta_out.tsv.gz"
  echo "    ${PHENOTYPE}: ${SRC_FILE}"
  gcs_cp "${SRC_FILE}" "${DST_FILE}"
  gcs_cp "${SRC_FILE}.tbi" "${DST_FILE}.tbi"
done

# ---------------------------------------------------------------------------
# Verify
# ---------------------------------------------------------------------------
if ! $DRY_RUN; then
  echo "==> Verifying copied files..."
  gcloud storage ls "${DST_BASE}/${MANIFEST_FILE}"
  gcloud storage ls "${DST_BASE}/summary_stats/"
  echo "Done. Test data is at: ${DST_BASE}"
fi
