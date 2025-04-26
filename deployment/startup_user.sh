#!/bin/bash

# install uv
curl -LsSf https://astral.sh/uv/install.sh | sh
# shellcheck disable=SC1091
source "$HOME/.local/bin/env"

# install package dependencies for ide
cd /opt/orchestration && uv sync --all-extras --dev
chgrp -R google-sudoers .venv
