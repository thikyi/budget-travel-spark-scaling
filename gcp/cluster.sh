#!/usr/bin/env bash
# cluster.sh - manage the one project cluster (guidelines Appendix A).
#   ./cluster.sh create 2     # create with 2 workers
#   ./cluster.sh resize 1     # change to 1 worker (only when no job is running)
#   ./cluster.sh workers      # print the current worker count
#   ./cluster.sh start        # restart after an idle auto-stop
#   ./cluster.sh delete       # delete when the session is done
# A cluster costs money while it exists. --stop-max-idle stops it after 1 idle hour
# (disks still cost a little); --max-age deletes it after 6 hours.
set -euo pipefail
cd "$(dirname "$0")"
source config.sh

case "${1:?create|resize|workers|start|delete}" in
  create)
    gcloud dataproc clusters create "$CLUSTER" \
      --region="$REGION" \
      --master-machine-type="$MASTER_MACHINE" --master-boot-disk-size="$MASTER_DISK_GB" \
      --worker-machine-type="$WORKER_MACHINE" --worker-boot-disk-size="$WORKER_DISK_GB" \
      --num-workers="${2:?number of workers}" \
      --image-version="$IMAGE_VERSION" \
      --optional-components=JUPYTER \
      --enable-component-gateway \
      --stop-max-idle=1h \
      --max-age=6h
    echo "Spark History Server / JupyterLab: Console > Dataproc > $CLUSTER > Web Interfaces" ;;
  resize)
    gcloud dataproc clusters update "$CLUSTER" --region="$REGION" --num-workers="${2:?number of workers}" ;;
  workers)
    gcloud dataproc clusters describe "$CLUSTER" --region="$REGION" \
      --format="value(config.workerConfig.numInstances)" ;;
  start)
    gcloud dataproc clusters start "$CLUSTER" --region="$REGION" ;;
  delete)
    gcloud dataproc clusters delete "$CLUSTER" --region="$REGION" --quiet ;;
esac
