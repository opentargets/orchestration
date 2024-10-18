SHELL := /bin/bash

PROJECT_ID ?= open-targets-genetics-dev
REGION ?= europe-west1
APP_NAME ?= $$(cat pyproject.toml| grep -m 1 "name" | cut -d" " -f3 | sed  's/"//g')
VERSION := $$(grep '^version' pyproject.toml | sed 's%version = "\(.*\)"%\1%')
BUCKET_NAME=gs://genetics_etl_python_playground/initialisation/${VERSION}/
DOCKER_IMAGE := "Orchestration-Airflow"

.PHONY: $(shell sed -n -e '/^$$/ { n ; /^[^ .\#][^ ]*:/ { s/:.*$$// ; p ; } ; }' $(MAKEFILE_LIST))
.DEFAULT_GOAL := help

dev: ## setup dev environment
	. setup-dev.sh

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

upload-ukb-ppp-bucket-readme: ## Upload ukb_ppp_eur_data readme to the bucket
	@gsutil rsync  docs/datasources/ukb_ppp_eur_data gs://ukb_ppp_eur_data/docs

upload-eqtl-catalogue-bucket-readme: ## Upload eqtl_catalogue_data readme to the bucket
	@gsutil rsync  docs/datasources/eqtl_catalogue_data gs://eqtl_catalogue_data/docs

upload-finngen-bucket-readme: ## Upload finngen_data readme to the bucket
	@gsutil rsync docs/datasources/finngen_data gs://finngen_data/docs

update-bucket-docs: upload-eqtl-catalogue-bucket-readme upload-ukb-ppp-bucket-readme upload-finngen-bucket-readme ## Upload readmes to the datasource buckets


build-gentropy-gcs-image: ## build image that overwrited gentropy with tools specific for orchestration and google cloud
	@docker buildx build \
		--platform=linux/amd64,linux/arm64 \
		-t europe-west1-docker.pkg.dev/open-targets-genetics-dev/gentropy-app/ot_gentropy:dev  \
		--push \
		-f images/gentropy/Dockerfile \
		--no-cache .
