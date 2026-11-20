#!/usr/bin/env bash
# setup.sh - run once in Cloud Shell.
# Turns on the Google Cloud services we need, creates the bucket and uploads the code.
# The 2 big raw CSVs are copied separately from the COMP541 repo (see README section 4).
#   ./setup.sh
# Run it again after every code change (git pull), so the bucket has the newest code.
set -euo pipefail
cd "$(dirname "$0")"
source config.sh

gcloud config set project "$PROJECT_ID"
gcloud services enable dataproc.googleapis.com compute.googleapis.com storage.googleapis.com

# Bucket in the same region as the cluster: faster reads and no network charges
gcloud storage buckets create "gs://$BUCKET" --location="$REGION" || echo "Bucket already exists - OK"

gcloud storage cp ../spark_app/*.py "gs://$BUCKET/code/"
gcloud storage cp ../lookup/vader_lexicon.txt ../lookup/model_ready_dataset.csv "gs://$BUCKET/lookup/"

echo "Done. Next: copy the raw CSVs to gs://$BUCKET/raw/airbnb/ and gs://$BUCKET/raw/tripadvisor/"
