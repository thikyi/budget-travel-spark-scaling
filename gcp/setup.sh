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

# The cluster VMs run as the default Compute Engine service account.
# New projects don't give it any roles, so give it what Dataproc needs:
# Dataproc Worker (to run the cluster) + read/write on our bucket (data and results).
PROJECT_NUMBER=$(gcloud projects describe "$PROJECT_ID" --format="value(projectNumber)")
VM_ACCOUNT="serviceAccount:${PROJECT_NUMBER}-compute@developer.gserviceaccount.com"
gcloud projects add-iam-policy-binding "$PROJECT_ID" --member="$VM_ACCOUNT" \
  --role="roles/dataproc.worker" --condition=None --quiet > /dev/null
gcloud storage buckets add-iam-policy-binding "gs://$BUCKET" --member="$VM_ACCOUNT" \
  --role="roles/storage.objectAdmin" > /dev/null

gcloud storage cp ../spark_app/*.py "gs://$BUCKET/code/"
gcloud storage cp ../lookup/vader_lexicon.txt ../lookup/model_ready_dataset.csv "gs://$BUCKET/lookup/"

echo "Done. Next: copy the raw CSVs to gs://$BUCKET/raw/airbnb/ and gs://$BUCKET/raw/tripadvisor/"
