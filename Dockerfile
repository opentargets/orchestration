FROM apache/airflow:slim-2.10.5-python3.12
USER root
ENV DEBIAN_FRONTEND=noninteractive
# Add g++
RUN apt-get update \
  && apt-get install -y --no-install-recommends g++ \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*
USER airflow
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/uv
COPY pyproject.toml README.md ./
# We have to install dependencies like this because uv does not seem to install
# `psycopg2-binary` properly when using the `--no-install-project` flag.

RUN uv pip compile pyproject.toml -o requirements.txt && uv pip install -r requirements.txt

USER 0
RUN curl -sSL "https://dl.google.com/dl/cloudsdk/channels/rapid/downloads/google-cloud-cli-linux-x86_64.tar.gz" | \
  tar -xzf - -C /tmp && \
  /tmp/google-cloud-sdk/install.sh --bash-completion=false --path-update=false --usage-reporting=false --quiet
USER airflow
