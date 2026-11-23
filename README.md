# Budget Travel Spark Scaling

In COMP 541 (Data Mining), our team built a project that ranks travel destinations by how much value you get for your money. We combined Airbnb prices, TripAdvisor reviews, cost of living and weather into one score that we call the Budget Value Index.

It worked, but it ran in pandas on a single machine.

For COMP 644 (Big Data), we rebuilt the whole pipeline in PySpark and moved it to Google Cloud Dataproc. Then we asked a simple question: **what happens when the data gets bigger, or when we add more machines?** This repo has the code we used to find out.

**Team:** Thant Thiri Myo Kyi, Angela Johnson, Annabel Tapia
**Course:** COMP 644 Big Data, California State University, Northridge, Fall 2026
**Instructor:** Prof. Mahdi Ebrahimi

## What we tested

We run the same Spark job again and again, and change only one thing at a time:

- **Experiment A, more data:** 2 workers stay fixed. The input grows from 100 MB to 500 MB, 1 GB and 2 GB.
- **Experiment B, more workers:** the input stays at 2 GB. The cluster grows from 1 to 2 to 5 workers.
- **Experiment C, more partitions (optional):** 2 GB and 2 workers stay fixed. We split the data into 4, 8, 16, 32 and 64 partitions.

Every setting runs 3 times, and we report the median. Before the timed runs on a fresh or resized cluster, we do one warm-up run that we don't count.

## What the Spark job does

The job has 4 stages, and we time each one:

1. **Lodging prices.** We clean about 1.56 million Airbnb listings and find the typical nightly price for each destination. This is a `groupBy`, so Spark has to shuffle data between machines.
2. **Review sentiment.** We split about 880,000 TripAdvisor reviews into words, join them with the VADER word list, and score each city. This is the heaviest stage: an `explode`, a broadcast join and two shuffles.
3. **Clustering.** We group the listings into 6 clusters with Spark MLlib K-Means. K-Means reads the data many times, so we cache it first.
4. **Ranking.** We join everything with the cost and weather table for 105 cities and sort by the Budget Value Index.

The timer covers reading the files from Cloud Storage, all 4 stages and writing the results. It doesn't include starting the cluster or starting Spark. Everything heavy happens in Spark. We only use pandas at the very end, to turn the small timing files into tables and graphs.

## About the data

The raw data comes from our COMP541 repo: Inside Airbnb listings, TripAdvisor reviews, and a cost and weather table we built back then. The cleaned original data is about 1.36 GB.

To get the sizes we needed, we resized it:

- **100 MB, 500 MB and 1 GB** are random samples of the real data.
- **2 GB and 5 GB** are copies of the real rows with new IDs, plus a sample for the rest.

The copied data is artificial. It makes Spark work harder, but it doesn't add any new information, and it can change analytical results like the K-Means clusters. So we use the resized datasets only to measure speed. To check that the ranking and the clusters are right, we run the job once more on the original data (`--size=original`).

One thing that surprised us early on: the raw CSV files have line breaks inside the review text. Spark can only read a file like that with one task, so nothing runs in parallel. `make_datasets.py` cleans this once and saves the data as normal CSV files of about 128 MB each, which Spark can split.

## What we found so far

**Experiment A (Nov 21):** the data grew 20.5 times (100 MB to 2 GB), but the median time grew only 4.6 times (45 s to 207 s).

| Size | Median time | Throughput |
|---|---|---|
| 100 MB | 45.3 s | 2.3 MB/s |
| 500 MB | 107.4 s | 4.9 MB/s |
| 1 GB | 163.0 s | 6.6 MB/s |
| 2 GB | 207.0 s | 10.4 MB/s |

Small data is slow for its size for two reasons. Every run pays a fixed start-up cost for each stage. And 100 MB is only 2 file pieces per table, so only 2 of our 4 worker cores had work to do. From 1 GB up, all 4 cores were busy. The review sentiment stage grew the most, and nothing spilled to disk.

Experiment B is almost done, and Experiment C is next. We'll add their results here when they're finished.

## The cluster

| | |
|---|---|
| Master | 1 × n4-standard-4 (4 vCPUs, 16 GB RAM) |
| Workers | n1-standard-2 (2 vCPUs, 7.5 GB RAM, 100 GB disk), 1, 2 or 5 of them |
| Software | Dataproc image 2.3 (Debian 12), Spark 3.5.3 |
| Region | us-central1 |
| Safety | stops after 1 idle hour, deletes itself after 6 hours |

We planned to use 10 workers too, but the free trial allows only 16 vCPUs at a time, and 10 workers would need 24. So 5 workers is our maximum.

## Folders

```
spark_app/   the Spark code: make_datasets.py (builds the sizes) and budget_value_job.py (the timed job)
gcp/         shell scripts: settings, setup, create/resize/delete the cluster, run experiments, collect results
analysis/    analyze_results.py turns the timing files into tables and graphs
lookup/      the VADER word list and the cost and weather table (105 cities)
test_sample/ small samples so you can try everything on a laptop first
```

All settings (machine types, sizes, worker counts, bucket name) live in `gcp/config.sh`.

## How to run it

You need a Google Cloud project with Dataproc turned on and a Cloud Storage bucket. If you use your own project, change `PROJECT_ID` and `BUCKET` in `gcp/config.sh` first.

**1. Get the code into Cloud Shell**

```bash
git clone https://github.com/thikyi/budget-travel-spark-scaling.git
cd budget-travel-spark-scaling/gcp
sed -i 's/\r$//' *.sh && chmod +x *.sh
./setup.sh        # project owner only: turns on the APIs, creates the bucket, uploads the code
```

**2. Copy the 2 raw CSV files into the bucket.** They're stored with Git LFS in our COMP541 repo:

```bash
cd ~
GIT_LFS_SKIP_SMUDGE=1 git clone --depth 1 https://github.com/thikyi/CSUN_COMP541_Group1_DataMiningProject_2026.git comp541
cd comp541
A="Group Project Phase 1 and 2 Gather Data & Exploration/Input/inside_airbnb_raw_combined.csv"
T="Group Project Phase 3 Data Preprocessing - Part 1/Input/TripAdvisor cleaned.csv"
git lfs pull --include="$A,$T"
source ~/budget-travel-spark-scaling/gcp/config.sh
gcloud storage cp "$A" gs://$BUCKET/raw/airbnb/
gcloud storage cp "$T" gs://$BUCKET/raw/tripadvisor/TripAdvisor_cleaned.csv
```

**3. Run the experiments.** Each session has to finish within 6 hours, because the cluster deletes itself after that.

```bash
cd ~/budget-travel-spark-scaling/gcp

./cluster.sh create 2
./run_experiments.sh make-data     # once: builds 100MB, 500MB, 1GB, 2GB and 5GB
./run_experiments.sh smoke         # a quick test on 100 MB
./run_experiments.sh expA          # Experiment A
./run_experiments.sh expC          # Experiment C (optional)
./run_experiments.sh validation    # one run on the original data, to check the results
./cluster.sh delete

./cluster.sh create 1
./run_experiments.sh expB          # Experiment B, 1 worker
./cluster.sh resize 5
./run_experiments.sh expB          # Experiment B, 5 workers
./cluster.sh delete

./collect_results.sh               # tables and graphs in analysis/results/
```

You can also submit the jobs one by one from the Dataproc page in the Google Cloud Console. That's what we did for most runs. Just make sure each argument is its own entry and starts with `--`, like `--size=2GB`.

**Try it on a laptop first (free).** With PySpark 3.5 installed:

```bash
mkdir -p tb/raw/airbnb tb/raw/tripadvisor tb/lookup
cp test_sample/airbnb_sample.csv tb/raw/airbnb/
cp test_sample/tripadvisor_sample.csv tb/raw/tripadvisor/
cp lookup/* tb/lookup/
spark-submit --master "local[4]" spark_app/make_datasets.py --bucket=tb --sizes=3MB,20MB
spark-submit --master "local[4]" spark_app/budget_value_job.py --bucket=tb --size=20MB --workers=1 --experiment=local_test
```

## Things that went wrong (so you don't have to repeat them)

- **The machine types in our plan weren't offered.** Our Console had no N2 machines and no Dataproc image 2.2, so we used N4 for the master and image 2.3. Same size, same Spark version family.
- **The first cluster wouldn't start.** New Google Cloud projects give the default compute service account no permissions. It needs the Dataproc Worker role, plus read and write access to the bucket. `setup.sh` now does this for you.
- **Don't use the Create cluster form.** Its defaults turn on auto-scaling, 500 GB SSD disks and Lightning Engine. That costs almost twice as much and changes the experiment. We always create the cluster with `cluster.sh`.
- **Typing job arguments by hand is easy to get wrong.** A missing `--` or a missing `=` makes the job fail in seconds. Annoying, but harmless.
- **A cluster machine restarted in the middle of a job** ("Node was restarted while executing a job"). Retrying on the same cluster didn't help. Deleting it, creating a fresh one, doing a warm-up and rerunning that one job did.
- **The Spark History Server link sometimes shows HTTP 502.** The "View in Spark UI" link on each job's page worked every time.

## Credits

This project builds on our COMP541 work, "Finding the Best Budget-Friendly Travel Destinations," and we thank our COMP541 teammates for the original analysis: [add names]. The data comes from Inside Airbnb, TripAdvisor reviews, and the VADER sentiment lexicon (Hutto and Gilbert, 2014).
