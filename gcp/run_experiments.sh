#!/usr/bin/env bash
# run_experiments.sh - submit the timed Spark job many times (one job at a time).
#
#   ./run_experiments.sh make-data     # once: build 100MB/500MB/1GB/2GB/5GB datasets
#   ./run_experiments.sh smoke         # one quick test run on 100MB
#   ./run_experiments.sh expA          # Exp A: 2 workers, all sizes, 3 reps   (cluster must have 2 workers)
#   ./run_experiments.sh expB          # Exp B: 2 GB, 3 reps on the current worker count (1, 2 or 5)
#   ./run_experiments.sh expC          # Exp C (bonus): 2 GB, 2 workers, partitions 4..64, 3 reps
#   ./run_experiments.sh validation    # once: the original (not resized) data, to check the ranking and clusters
#
# Built-in rules (guidelines section 5):
#   - the script reads the real worker count from the cluster, so the record is always correct
#   - one warm-up run after every create/resize (not counted)
#   - repeats are the outer loop, so time-of-day noise is spread out
#   - cluster start-up and resize time are never inside the measured job time
set -euo pipefail
cd "$(dirname "$0")"
source config.sh

current_workers() { ./cluster.sh workers; }

submit() {  # submit <script> <args...>
  local script=$1; shift
  gcloud dataproc jobs submit pyspark "gs://$BUCKET/code/$script" \
    --cluster="$CLUSTER" --region="$REGION" -- --bucket="gs://$BUCKET" "$@"
}

need_workers() {  # stop if the cluster does not have the worker count this experiment needs
  local want=$1 have; have=$(current_workers)
  if [[ "$have" != "$want" ]]; then
    echo "Cluster has $have workers, this step needs $want. Run: ./cluster.sh resize $want"; exit 1
  fi
}

warmup() { submit budget_value_job.py --size=100MB --workers="$1" --experiment=warmup --rep=0; }

case "${1:?make-data|smoke|expA|expB|expC|validation}" in
  make-data)
    submit make_datasets.py --sizes="$DATA_SIZES" ;;
  validation)
    W=$(current_workers)
    submit budget_value_job.py --size=original --workers="$W" --experiment=validation --rep=1 ;;
  smoke)
    W=$(current_workers)
    submit budget_value_job.py --size=100MB --workers="$W" --experiment=smoke --rep=1 ;;
  expA)
    need_workers "$EXPA_WORKERS"; warmup "$EXPA_WORKERS"
    for rep in $(seq 1 "$REPS"); do
      for size in $EXPA_SIZES; do
        submit budget_value_job.py --size="$size" --workers="$EXPA_WORKERS" --experiment=expA --rep="$rep"
      done
    done ;;
  expB)
    W=$(current_workers); warmup "$W"
    if [[ "$W" == "$EXPA_WORKERS" ]]; then
      echo "Note: the 2 GB / $W-worker runs from Exp A already count as B2. Running $REPS more anyway."
    fi
    for rep in $(seq 1 "$REPS"); do
      submit budget_value_job.py --size="$EXPB_SIZE" --workers="$W" --experiment=expB --rep="$rep"
    done ;;
  expC)
    need_workers "$EXPA_WORKERS"; warmup "$EXPA_WORKERS"
    for rep in $(seq 1 "$REPS"); do
      for n in $EXPC_PARTITIONS; do
        submit budget_value_job.py --size="$EXPC_SIZE" --workers="$EXPA_WORKERS" \
               --experiment=expC --partitions="$n" --rep="$rep"
      done
    done ;;
esac
