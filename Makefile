PROJECT_ID ?= open-targets-genetics-dev
REGION ?= europe-west1
APP_NAME ?= $$(cat pyproject.toml| grep -m 1 "name" | cut -d" " -f3 | sed  's/"//g')
VERSION_NO ?= $$(poetry version --short)
CLEAN_VERSION_NO := $(shell echo "$(VERSION_NO)" | tr -cd '[:alnum:]')
BUCKET_NAME=gs://genetics_etl_python_playground/initialisation/${VERSION_NO}/
BUCKET_COMPOSER_DAGS=gs://europe-west1-ot-workflows-fe147745-bucket/dags/
.PHONY: $(shell sed -n -e '/^$$/ { n ; /^[^ .\#][^ ]*:/ { s/:.*$$// ; p ; } ; }' $(MAKEFILE_LIST))
DOCKER_IMAGE := "Orchestration-Airflow"


.DEFAULT_GOAL := help

dev: # setup dev environment
	./setup-dev.sh


DIST_DIR := .
# could be referred with oetry version | awk '{print $2}, but then requires poetry to be there
VERSION := $$(grep '^version' pyproject.toml | sed 's%version = "\(.*\)"%\1%')


help: ## This is help
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)


version: ## display version and exit
	@echo $(VERSION)


build-orchestration-local: ## build orchestration package locally
	@poetry build -q -o $(DIST_DIR) -f wheel


get-orchestration-local-dist: build-orchestration-local
	@ls $(DIST_DIR) | grep $(VERSION)
	
.ONESHELL:
build-airflow-image:  ## build local airflow image for the infrastructure
	# get the orchestration package distribution wheel path after package is build
	@$(eval ORCHESTRATION_PACKAGE = $(shell $(MAKE) get-orchestration-local-dist))
	docker build . \
		--tag extending_airflow:latest \
		--build-arg ORCHESTRATION_PACKAGE=$(ORCHESTRATION_PACKAGE) \
		-f Dockerfile \
		--no-cache
compile-config:  ## compile config
	@jsonnet config/config.jsonnet
