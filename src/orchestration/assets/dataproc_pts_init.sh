#!/usr/bin/env bash

set -euo pipefail

set -x



PTS_REF=$(/usr/share/google/get_metadata_value attributes/PTS_REF)
readonly PTS_REF
readonly REPO_URI="https://github.com/opentargets/pts"
DATAPROC_CLUSTER_NAME=$(/usr/share/google/get_metadata_value attributes/dataproc-cluster-name)
readonly DATAPROC_CLUSTER_NAME
echo "export DATAPROC_CLUSTER_NAME=${DATAPROC_CLUSTER_NAME}" >> /etc/profile.d/custom_env_vars.sh

function err() {
    echo "[$(date +'%Y-%m-%dT%H:%M:%S%z')]: $*" >&2
    exit 1
}

function run_with_retry() {
    local -r cmd=("$@")
    for ((i = 0; i < 3; i++)); do
        if "${cmd[@]}"; then
            return 0
        fi
        sleep 5
    done
    err "Failed to run command: ${cmd[*]}"
}

function install_pip() {
    if command -v pip >/dev/null; then
        echo "pip is already installed."
        return 0
    fi

    if command -v easy_install >/dev/null; then
        echo "Installing pip with easy_install..."
        run_with_retry easy_install pip
        return 0
    fi

    echo "Installing python-pip..."
    run_with_retry apt update
    run_with_retry apt install python-pip -y
}


function main() {
    if [[ -z "${PTS_REF}" ]]; then
        echo "ERROR: Must specify PTS_REF metadata key"
        exit 1
    fi
    install_pip
    pip install uv

    pip uninstall -y pts
    echo "Install package..."
    run_with_retry uv pip install --no-break-system-packages --system "pts @ git+${REPO_URI}.git@${PTS_REF}"
}

main
