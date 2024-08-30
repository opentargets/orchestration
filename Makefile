SHELL := /bin/bash

PROJECT_ID ?= open-targets-genetics-dev
REGION ?= europe-west1
APP_NAME ?= $$(cat pyproject.toml| grep -m 1 "name" | cut -d" " -f3 | sed  's/"//g')
VERSION := $$(grep '^version' pyproject.toml | sed 's%version = "\(.*\)"%\1%')
BUCKET_NAME=gs://genetics_etl_python_playground/initialisation/${VERSION}/
DOCKER_IMAGE := "Orchestration-Airflow"

.PHONY: $(shell sed -n -e '/^$$/ { n ; /^[^ .\#][^ ]*:/ { s/:.*$$// ; p ; } ; }' $(MAKEFILE_LIST))
.DEFAULT_GOAL := help

dev: # setup dev environment
	source setup-dev.sh

app:
	echo src/$(APP_NAME)

help: ## This is help
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)

version: ## display version and exit
	@echo $(VERSION)

check-types: ## run mypy and check types
	@poetry run python -m mypy --install-types --non-interactive src/$(APP_NAME)

format: ## run formatting
	@poetry run python -m ruff check --fix  src/$(APP_NAME) tests

test: ## run unit tests
	@poetry run python -m pytest tests/*.py

check: format check-types test ## run all checks

generate-requirements: ## generate requirements.txt from poetry dependencies to install in the docker image
	poetry export --without-hashes --with dev --format=requirements.txt > requirements.txt

build-airflow-image: generate-requirements  ## build local airflow image for the infrastructure
	docker build . \
		--tag extending_airflow:latest \
		-f Dockerfile \
		--no-cache
