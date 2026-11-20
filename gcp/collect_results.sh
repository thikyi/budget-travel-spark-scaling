#!/usr/bin/env bash
# collect_results.sh - download every run's METRICS_JSON into one file, then make tables + graphs.
#   ./collect_results.sh
set -euo pipefail
cd "$(dirname "$0")"
source config.sh

gcloud storage cat "gs://$BUCKET/results/metrics/*/part-*" \
  | grep -v -E '"experiment": "(warmup|smoke)"' > ../analysis/metrics.jsonl
echo "runs collected: $(wc -l < ../analysis/metrics.jsonl)"

cd ../analysis
pip install -q pandas matplotlib tabulate
python analyze_results.py metrics.jsonl \
  --expA-workers "$EXPA_WORKERS" --expB-size "$EXPB_SIZE" --bonus-size "$BONUS_SIZE"
