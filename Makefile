## Parameters for the Dataflow Job for an easy RUN
## Change the following parameters to run the Dataflow Job on your own data and GCP Project
BUCKET_TO_SEARCH ?= bqtrialbucket
PARQUET_PATH ?= new_data
DATASET ?= yelp
TABLE ?= attributes_2
GCP_PROJECT ?= bqtrial-383917
GCP_REGION ?= us-central1
TEMPLATE_NAME ?= pipeline-trial
TEMPLATE_TAG ?= 1.2.0

## Parameters for the Dataflow Container
PROJECT_NUMBER ?= $$(gcloud projects list --filter=${GCP_PROJECT} --format="value(PROJECT_NUMBER)")
GCS_PATH ?= gs://${GCP_PROJECT}-dataflow-${PROJECT_NUMBER}
TEMPLATE_PATH ?= ${GCS_PATH}/templates/${TEMPLATE_NAME}
TEMPLATE_IMAGE ?= ${GCP_REGION}-docker.pkg.dev/${GCP_PROJECT}/${TEMPLATE_NAME}/${TEMPLATE_NAME}:${TEMPLATE_TAG}
JOB_NAME=pipeline-trial-$$(date +%Y%m%dt%H%M%S)
OUTPUT=${GCS_PATH}/out/${JOB_NAME}/output
WORKDIR ?= /opt/dataflow
FLEX_TEMPLATE_PYTHON_PY_FILE ?= ${WORKDIR}/$$(echo ${TEMPLATE_NAME}|tr '-' '_')/main.py
FLEX_TEMPLATE_PYTHON_SETUP_FILE ?= ${WORKDIR}/setup.py

.PHONY: $(shell sed -n -e '/^$$/ { n ; /^[^ .\#][^ ]*:/ { s/:.*$$// ; p ; } ; }' $(MAKEFILE_LIST))

.DEFAULT_GOAL := help

help: ## This is help
	@echo ${MAKEFILE_LIST}
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)

init: ## Build Bucket for Demo and the Artifact Registry -- Run this One time Only!
	@gcloud config set project ${GCP_PROJECT}
	@echo "Enabling Private Google Access in Region ${GCP_REGION} ......"
	@gcloud compute networks subnets update default \
	--region=${GCP_REGION} \
	--enable-private-ip-google-access
	@echo "Enabling Dataflow Service...." && gcloud services enable dataflow --project ${GCP_PROJECT}
	@echo "Enabling Artifact Registry..." && gcloud services enable artifactregistry.googleapis.com --project ${GCP_PROJECT}
	@echo "Building Bucket to Store template...." && gsutil mb -c standard -l ${GCP_REGION} -p ${GCP_PROJECT} ${GCS_PATH}
	@echo "Building Artifact Repo to Store Docker Image of Code...." && gcloud artifacts repositories create ${TEMPLATE_NAME} \
    --repository-format=docker \
    --location=${GCP_REGION} \
    --async

template: ## Build Flex Template Container and Upload Container to GCS Bucket
	gcloud config set project ${GCP_PROJECT}
	# Build DataFlow Container and Upload to Artifact Registry
	gcloud config set builds/use_kaniko True
	gcloud config set builds/kaniko_cache_ttl 480
	gcloud builds submit --tag ${TEMPLATE_IMAGE} .
	# Build Data Flex Template and Upload to GCS
	gcloud dataflow flex-template build ${GCS_PATH}/templates/${TEMPLATE_TAG}/${TEMPLATE_NAME}.json \
    --image ${TEMPLATE_IMAGE} \
    --sdk-language "PYTHON" \
    --metadata-file ${TEMPLATE_NAME}-metadata.json

run: ## Run the Dataflow Container
	gcloud config set project ${GCP_PROJECT}
	gcloud dataflow flex-template run ${JOB_NAME} \
    --template-file-gcs-location ${GCS_PATH}/templates/${TEMPLATE_TAG}/${TEMPLATE_NAME}.json \
    --region ${GCP_REGION} \
    --staging-location ${GCS_PATH}/staging \
	--temp-location ${GCS_PATH}/temp \
    --parameters project="${GCP_PROJECT}",bucket="${BUCKET_TO_SEARCH}",parquetpath="${PARQUET_PATH}",dataset="${DATASET}",table="${TABLE}"