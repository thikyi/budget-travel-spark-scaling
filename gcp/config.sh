#!/usr/bin/env bash
# config.sh - one place for all settings. Every other script does: source config.sh
# Values follow "COMP644 Group Project Guidelines (With Bonus)". Keep them the same for all runs.

export PROJECT_ID="comp644-group-project-514403"   # from the Console welcome page
export BUCKET="comp644-budget-travel-514403"       # bucket names are global; change if "already taken"

export REGION="us-central1"                        # zone = automatic (guidelines section 2)
export IMAGE_VERSION="2.3-debian12"                # Dataproc image 2.3 on Debian 12 (Spark 3.5)
export CLUSTER="comp644-project"                   # one cluster, resized between experiments

# Machines (guidelines section 2) - never change mid-project
export MASTER_MACHINE="n4-standard-4"              # 4 vCPU, 16 GiB (N4 uses a Hyperdisk boot disk)
export MASTER_DISK_GB=50
export WORKER_MACHINE="n1-standard-2"              # 2 vCPU, 7.5 GiB each
export WORKER_DISK_GB=100

# All dataset sizes built once by make_datasets.py (5GB is optional)
export DATA_SIZES="100MB,500MB,1GB,2GB,5GB"

# Experiment A - input data scalability: cluster fixed at 1 master + 2 workers
export EXPA_WORKERS=2
export EXPA_SIZES="100MB 500MB 1GB 2GB"            # add 5GB only if the instructor approves

# Experiment B - worker scalability: data fixed at 2 GB, workers 1, 2, 5
export EXPB_SIZE="2GB"
export EXPB_WORKERS="1 2 5"                        # 10 = optional, needs quota + approval

# Experiment C (the optional bonus) - partition scalability: 2 GB, 1 master + 2 workers, only the partition count changes
export EXPC_SIZE="2GB"
export EXPC_PARTITIONS="4 8 16 32 64"

export REPS=3                                      # 3 runs per point; report all runs + the median
