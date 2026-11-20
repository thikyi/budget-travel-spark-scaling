# Budget Travel Spark Scaling

**How well does a real analytics pipeline scale on Apache Spark?** This project runs a budget-travel
analytics pipeline on Google Cloud Dataproc and measures what happens when (A) the data grows from
100 MB to 2 GB and (B) the cluster grows from 1 to 5 workers. An optional study (C) tests how the number
of Spark partitions changes performance.

The pipeline ranks travel destinations by a **Budget Value Index**, using 1.56M Airbnb listings,
0.88M TripAdvisor reviews (VADER sentiment), and Spark MLlib K-Means clustering. It is our COMP541 data
mining project, rebuilt in PySpark.

**Team:** Thant Thiri Myo Kyi · Angela Johnson · Annabel Tapia
**Course:** COMP 644 Big Data – group project, California State University, Northridge (Fall 2026)
**Stack:** PySpark 3.5 · Spark MLlib · Google Cloud Dataproc 2.2 · Cloud Storage · Bash


## 1. Folder map

```
Project/
├── README.md
├── spark_app/
│   ├── make_datasets.py       <- one-time: clean raw CSVs + build 100MB/500MB/1GB/2GB/5GB datasets
│   └── budget_value_job.py    <- THE timed Spark job (4 stages, prints METRICS_JSON; --partitions for the bonus)
├── gcp/
│   ├── config.sh              <- all settings (guideline machine types, sizes, worker counts)
│   ├── setup.sh               <- turn on APIs, make the bucket, upload code
│   ├── cluster.sh             <- create / resize / start / delete the ONE project cluster
│   ├── run_experiments.sh     <- make-data, smoke, expA, expB, bonus
│   └── collect_results.sh     <- download metrics + make all tables and graphs
├── analysis/
│   └── analyze_results.py     <- raw timing table, medians, speedup, efficiency, cost, 5 graphs + bonus graph
├── lookup/                    <- VADER word list + COMP541 cost/weather table (105 cities)
├── test_sample/               <- 1% samples to test on a laptop first
└── .gitignore                 <- keeps "Working folder/" (notes, drafts, report) out of git
```

## 2. The Spark job

| Stage | What happens | Spark idea |
|---|---|---|
| **S1 lodging** | 1.56M Airbnb listings → clean price → median price per destination | `groupBy` = wide transformation → shuffle |
| **S2 sentiment** | 0.88M TripAdvisor reviews → words → `explode` → join VADER word list → score per review → average per city | broadcast join, 2 shuffles |
| **S3 K-Means** | Spark MLlib K-Means (k = 6) on listing features | iterative algorithm, cache |
| **S4 ranking** | join cost + weather (105 cities) + S1 + S2 → Budget Value Index → sort | broadcast joins, global sort |

**Measured time** = S1 + S2 + S3 + S4. It includes reading the CSVs from the bucket, all processing,
K-Means training and writing each stage's output. It does **not** include cluster start-up/resize or
Spark session start-up (logged separately). In the bonus runs it also includes the `repartition(N)` shuffle.

Each run also records: input bytes, input partitions, active executors, executor cores/memory,
Spark version, and the Spark UI totals (tasks, shuffle read/write, memory/disk spill).

## 3. Cluster (guidelines section 2 + Appendix A)

| | Value |
|---|---|
| Master | n2-standard-4 (4 vCPU, 16 GiB), 50 GB disk |
| Workers | n1-standard-2 (2 vCPU, 7.5 GiB), 100 GB disk, **1, 2 or 5** |
| Region | us-central1, automatic zone |
| Software | Dataproc image 2.2 (Spark 3.5), JupyterLab component, component gateway |
| Safety | `--stop-max-idle=1h` (auto-stop), `--max-age=6h` (auto-delete) |
| vCPUs used | 4 + 2 × workers → 6 / 8 / 14. Free-trial limit = 16, so **10 workers (24) is not possible** |

## 4. Step by step (Cloud Shell)

```bash
git clone https://github.com/thikyi/budget-travel-spark-scaling.git Project   # password = GitHub token
cd Project/gcp && sed -i 's/\r$//' *.sh && chmod +x *.sh
./setup.sh                                    # APIs + bucket + code

# raw CSVs from the public COMP541 repo (Git LFS) -> bucket
cd ~ && (git lfs version || sudo apt-get install -y git-lfs) && git lfs install
GIT_LFS_SKIP_SMUDGE=1 git clone --depth 1 https://github.com/thikyi/CSUN_COMP541_Group1_DataMiningProject_2026.git comp541
cd comp541
A="Group Project Phase 1 and 2 Gather Data & Exploration/Input/inside_airbnb_raw_combined.csv"
T="Group Project Phase 3 Data Preprocessing - Part 1/Input/TripAdvisor cleaned.csv"
git lfs pull --include="$A,$T" && ls -lh "$A" "$T"
source ~/Project/gcp/config.sh
gcloud storage cp "$A" gs://$BUCKET/raw/airbnb/
gcloud storage cp "$T" gs://$BUCKET/raw/tripadvisor/TripAdvisor_cleaned.csv
cd ~ && rm -rf comp541 && cd ~/Project/gcp

# Each session must finish within 6 hours (--max-age deletes the cluster after that).

# Session 1 - 2 workers: datasets, smoke test, Experiment A
./cluster.sh create 2
./run_experiments.sh make-data
./run_experiments.sh smoke
./run_experiments.sh expA          # 4 sizes x 3 runs; the 2 GB runs also count as B2
./cluster.sh delete

# Session 2 (optional bonus) - 2 workers, 2 GB, partitions 4/8/16/32/64 x 3 runs
./cluster.sh create 2
./run_experiments.sh bonus
./cluster.sh delete

# Session 3 - worker scaling at 2 GB
./cluster.sh create 1
./run_experiments.sh expB          # B1 = 1 worker
./cluster.sh resize 5
./run_experiments.sh expB          # B3 = 5 workers
./cluster.sh delete

./collect_results.sh               # -> analysis/results/*.csv, *.png, summary.md
```

Run long loops inside `tmux` and keep the Cloud Shell tab open. If Cloud Shell disconnects,
finished runs are safe in the bucket; resubmit only the missing ones.

## 5. Test on a laptop first (no cloud cost)

```bash
pip install pyspark==3.5.3 pandas matplotlib tabulate
mkdir -p tb/raw/airbnb tb/raw/tripadvisor tb/lookup
cp test_sample/airbnb_sample.csv tb/raw/airbnb/ && cp test_sample/tripadvisor_sample.csv tb/raw/tripadvisor/ && cp lookup/* tb/lookup/
spark-submit --master "local[4]" spark_app/make_datasets.py --bucket=tb --sizes=3MB,20MB
spark-submit --master "local[4]" spark_app/budget_value_job.py --bucket=tb --size=20MB --workers=1 --experiment=local_test
spark-submit --master "local[4]" spark_app/budget_value_job.py --bucket=tb --size=20MB --workers=1 --experiment=local_test --partitions=8
```

## 6. Rules for fair measurements (guidelines section 5)

- Same application, machine types, image, data format (CSV, uncompressed) and Spark settings for every run.
- 3 runs per point; report **all runs and the median**; explain any missing or failed run.
- One warm-up run after every create/resize (not counted). Only one cluster at a time.
- Check correctness: the same input gives the same ranking in every run.
- Find and explain at least **one unexpected result** with Spark UI evidence.
