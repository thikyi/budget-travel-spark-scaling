# budget_value_job.py
# The timed Spark job for the scalability experiments.
#
# It ranks travel destinations by a Budget Value Index (BVI), the same idea and
# weights as our COMP541 project, but written in PySpark so it runs on many workers.
#
# The job has 4 stages. Each stage ends with a write (a Spark action), and we time
# each stage on its own:
#   S1 lodging    Airbnb listings -> median price per destination (groupBy = shuffle)
#   S2 sentiment  TripAdvisor reviews -> words -> VADER score per review -> average per city
#   S3 kmeans     K-Means clustering of the listings with Spark MLlib
#   S4 ranking    join everything to the 105-city table -> BVI -> sorted ranking
#
# The measured time includes reading the CSVs, all processing, K-Means training and
# writing each stage's output. It does not include cluster start-up or Spark start-up.
#
# Example (Dataproc):
#   gcloud dataproc jobs submit pyspark budget_value_job.py --cluster=comp644-project \
#       --region=us-central1 -- --bucket=gs://my-bucket --size=1GB --workers=2 --experiment=expA --rep=1
#
# Note: this file and make_datasets.py each have their own small helpers (CSV options,
# folder size). We keep that small copy on purpose, so each file runs on Dataproc alone.

import argparse
import json
import time
import urllib.request
import uuid
from datetime import datetime, timezone

from pyspark.ml import Pipeline
from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import StandardScaler, VectorAssembler
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# BVI weights from COMP541 (9_Data_Modeling.ipynb)
WEIGHT_COST = 0.45
WEIGHT_SATISFACTION = 0.10
WEIGHT_WEATHER = 0.45

# VADER turns a sum of word scores into -1..1 with: score / sqrt(score^2 + 15)
VADER_ALPHA = 15.0

MAX_PRICE = 5000          # nightly prices above this are treated as data errors
KMEANS_K = 6              # number of clusters (same as COMP541)
KMEANS_MAX_ITER = 20
RANDOM_SEED = 42
BYTES_PER_MB = 1_000_000  # decimal MB, the same unit the Spark UI shows

KMEANS_FEATURES = ["latitude", "longitude", "log_price", "reviews_per_month",
                   "availability_365", "minimum_nights"]

# quote/escape '"' = standard CSV, where a quote inside text is written as ""
CSV_OPTIONS = {"header": "true", "quote": '"', "escape": '"'}


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--bucket", required=True, help="gs://bucket, or a local folder for testing")
    parser.add_argument("--size", required=True, help="dataset label, e.g. 100MB or 2GB")
    parser.add_argument("--workers", type=int, required=True, help="worker count, for the record")
    parser.add_argument("--experiment", default="expA", help="expA, expB, bonus, smoke or warmup")
    parser.add_argument("--rep", type=int, default=1, help="repeat number (1-3)")
    parser.add_argument("--partitions", type=int, default=0,
                        help="bonus only: repartition both inputs to N partitions (0 = off)")
    return parser.parse_args()


def read_csv(spark, path):
    # No inferSchema: it reads the data twice and would add noise to the timing.
    return spark.read.options(**CSV_OPTIONS).csv(path)


def clean_key(column):
    # lower case + trim + drop a leading "the ", so "The Netherlands" matches "netherlands"
    return F.regexp_replace(F.lower(F.trim(column)), r"^the ", "")


def with_clean_price(listings):
    # "$1,234.00" -> 1234.0, then drop missing and unrealistic prices
    price = F.regexp_replace("price", r"[$,]", "").cast("double")
    return (listings
            .withColumn("price_num", price)
            .filter((F.col("price_num") > 0) & (F.col("price_num") < MAX_PRICE)))


# S1: lodging price per destination

def lodging_summary(listings):
    listings = (with_clean_price(listings)
                .withColumn("city_key", clean_key(F.col("destination")))
                .withColumn("country_key", clean_key(F.col("country"))))

    # groupBy is a wide transformation: Spark shuffles rows so each city lands on one task
    return (listings
            .groupBy("city_key", "country_key")
            .agg(F.count("*").alias("n_listings"),
                 F.percentile_approx("price_num", 0.5).alias("median_price_local"),
                 F.avg(F.col("reviews_per_month").cast("double")).alias("avg_reviews_per_month"),
                 F.avg(F.col("availability_365").cast("double")).alias("avg_availability_365")))


# S2: review sentiment per city

def review_words(reviews):
    # The raw City value looks like:  'New York City'}   -> remove the quotes and braces
    city = clean_key(F.regexp_replace(F.col("City"), r"['{}]", ""))
    has_city = city.isNotNull() & (city != "") & (city != "locality")

    # One row per word. This makes the data about 150x longer: the heavy part of the job.
    words = F.explode(F.split(F.lower(F.col("text")), r"[^a-z']+"))
    return (reviews
            .withColumn("city_key", city)
            .filter(has_city)
            .withColumn("review_id", F.monotonically_increasing_id())
            .withColumn("word", words))


def sentiment_summary(reviews, lexicon):
    # The VADER word list is small (~7,500 words), so we broadcast it: every worker gets
    # a full copy and the huge words table does not need to be shuffled for this join.
    scored_words = review_words(reviews).join(F.broadcast(lexicon), on="word", how="inner")

    # Shuffle 1: add up the word scores of each review
    per_review = (scored_words
                  .groupBy("review_id", "city_key")
                  .agg(F.sum("valence").alias("raw_score"))
                  .withColumn("compound",
                              F.col("raw_score") / F.sqrt(F.col("raw_score") ** 2 + VADER_ALPHA)))

    # Shuffle 2: average per city, then move -1..1 to 0..1
    return (per_review
            .groupBy("city_key")
            .agg(F.count("*").alias("n_reviews_scored"),
                 F.avg("compound").alias("avg_compound"))
            .withColumn("satisfaction_0_1", (F.col("avg_compound") + 1) / 2)
            .withColumn("country_key", F.lit("united states")))  # TripAdvisor data = US hotels


# S3: K-Means clustering of listings

def kmeans_features(listings):
    return (with_clean_price(listings)
            .withColumn("log_price", F.log1p("price_num"))
            .select([F.col(name).cast("double").alias(name) for name in KMEANS_FEATURES])
            .na.fill(0.0))


def run_kmeans(listings):
    # K-Means reads the data once per iteration, so we cache it in memory first
    features = kmeans_features(listings).cache()
    pipeline = Pipeline(stages=[
        VectorAssembler(inputCols=KMEANS_FEATURES, outputCol="raw_features"),
        StandardScaler(inputCol="raw_features", outputCol="features"),
        KMeans(k=KMEANS_K, maxIter=KMEANS_MAX_ITER, seed=RANDOM_SEED, featuresCol="features"),
    ])
    summary = pipeline.fit(features).stages[-1].summary  # fit() is the Spark action here
    features.unpersist()
    return {"k": KMEANS_K, "iterations": summary.numIter, "cluster_sizes": summary.clusterSizes,
            "training_cost": round(summary.trainingCost, 2)}


# S4: Budget Value Index ranking

def ranking_pool(destinations, lodging, sentiment):
    pool = (destinations
            .filter(F.col("in_ranking_pool").cast("int") == 1)
            .select("city", "country", "city_key", "country_key",
                    F.col("cost_of_living_composite").cast("double").alias("cost"),
                    F.col("avg_weather_score").cast("double").alias("weather")))
    keys = ["city_key", "country_key"]
    sentiment = sentiment.select(*keys, "satisfaction_0_1", "n_reviews_scored")
    # Both results have about 100 rows, so broadcast joins are cheap
    return (pool
            .join(F.broadcast(lodging), keys, "left")
            .join(F.broadcast(sentiment), keys, "left"))


def bvi_ranking(destinations, lodging, sentiment):
    pool = ranking_pool(destinations, lodging, sentiment)

    # Min-max scaling needs the min and max of the whole pool (one small summary row)
    stats = pool.agg(F.min("cost").alias("cost_min"), F.max("cost").alias("cost_max"),
                     F.min("weather").alias("weather_min"), F.max("weather").alias("weather_max"),
                     F.percentile_approx("satisfaction_0_1", 0.5).alias("satisfaction_median"))
    cost_scaled = (F.col("cost") - F.col("cost_min")) / (F.col("cost_max") - F.col("cost_min"))
    weather_scaled = (F.col("weather") - F.col("weather_min")) / (F.col("weather_max") - F.col("weather_min"))
    # Cities without reviews get the median satisfaction (same rule as COMP541)
    satisfaction = F.coalesce("satisfaction_0_1", "satisfaction_median")

    bvi = (WEIGHT_COST * (1 - cost_scaled)
           + WEIGHT_SATISFACTION * satisfaction
           + WEIGHT_WEATHER * weather_scaled)
    return (pool.crossJoin(F.broadcast(stats))
            .withColumn("bvi_full", F.round(bvi, 3))
            .select("city", "country", "bvi_full", "cost", "weather", "satisfaction_0_1",
                    "n_listings", "median_price_local", "n_reviews_scored")
            .orderBy(F.desc("bvi_full")))


# The run

def load_inputs(spark, bucket, size, partitions):
    data_dir = f"{bucket}/data/size={size}"
    listings = read_csv(spark, f"{data_dir}/airbnb")
    reviews = read_csv(spark, f"{data_dir}/reviews")
    if partitions:
        # Bonus experiment: the only place where the partition count is changed
        listings = listings.repartition(partitions)
        reviews = reviews.repartition(partitions)

    lexicon = (spark.read.option("sep", "\t").csv(f"{bucket}/lookup/vader_lexicon.txt")
               .select(F.col("_c0").alias("word"), F.col("_c1").cast("double").alias("valence")))
    destinations = read_csv(spark, f"{bucket}/lookup/model_ready_dataset.csv")
    return listings, reviews, lexicon, destinations


def save_and_reload(spark, df, path):
    # Writing is the Spark action that makes the lazy plan run. Reading the small result
    # back means the next stage starts from the saved file, not from a re-run of this stage.
    df.write.mode("overwrite").parquet(path)
    return spark.read.parquet(path)


def seconds_since(start):
    return round(time.perf_counter() - start, 3)


def run_stages(spark, inputs, output_dir):
    listings, reviews, lexicon, destinations = inputs
    stage_seconds = {}

    start = time.perf_counter()
    lodging = save_and_reload(spark, lodging_summary(listings), f"{output_dir}/lodging")
    stage_seconds["s1_lodging"] = seconds_since(start)

    start = time.perf_counter()
    sentiment = save_and_reload(spark, sentiment_summary(reviews, lexicon), f"{output_dir}/sentiment")
    stage_seconds["s2_sentiment"] = seconds_since(start)

    start = time.perf_counter()
    kmeans_info = run_kmeans(listings)
    stage_seconds["s3_kmeans"] = seconds_since(start)

    start = time.perf_counter()
    ranking = bvi_ranking(destinations, lodging, sentiment)
    ranking.coalesce(1).write.mode("overwrite").options(**CSV_OPTIONS).csv(f"{output_dir}/ranking")
    stage_seconds["s4_ranking"] = seconds_since(start)

    for stage, seconds in stage_seconds.items():
        print(f"[timer] {stage}: {seconds} s")
    return stage_seconds, kmeans_info


# Run details for the report

def folder_size_bytes(spark, path):
    # Asks the file system (Cloud Storage or local disk) for the total size of a folder
    jvm = spark.sparkContext._jvm
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    folder = jvm.org.apache.hadoop.fs.Path(path)
    return folder.getFileSystem(hadoop_conf).getContentSummary(folder).getLength()


def active_executors(spark_context):
    # The list includes the driver, so subtract 1
    return spark_context._jsc.sc().getExecutorMemoryStatus().size() - 1


def spark_ui_totals(spark_context):
    # The same numbers as the Spark UI Stages tab, read from Spark's REST API.
    # Monitoring must never fail the run, so any error is just recorded.
    url = (f"{spark_context.uiWebUrl}/api/v1/applications/"
           f"{spark_context.applicationId}/stages?status=complete")
    try:
        with urllib.request.urlopen(url, timeout=10) as response:
            stages = json.load(response)
    except Exception as error:
        return {"error": str(error)[:200]}

    def total(field):
        return sum(int(stage.get(field) or 0) for stage in stages)

    return {
        "stages": len(stages),
        "tasks": total("numCompleteTasks"),
        "max_tasks_in_a_stage": max((int(stage.get("numTasks") or 0) for stage in stages), default=0),
        "shuffle_read_mb": round(total("shuffleReadBytes") / BYTES_PER_MB, 1),
        "shuffle_write_mb": round(total("shuffleWriteBytes") / BYTES_PER_MB, 1),
        "memory_spill_mb": round(total("memoryBytesSpilled") / BYTES_PER_MB, 1),
        "disk_spill_mb": round(total("diskBytesSpilled") / BYTES_PER_MB, 1),
    }


def spark_settings(spark_context):
    conf = dict(spark_context.getConf().getAll())
    return {
        "default_parallelism": spark_context.defaultParallelism,
        "shuffle_partitions": conf.get("spark.sql.shuffle.partitions", "200"),
        "aqe_enabled": conf.get("spark.sql.adaptive.enabled", "default"),
        "dynamic_allocation": conf.get("spark.dynamicAllocation.enabled", "default"),
        "executor_cores": conf.get("spark.executor.cores", "default"),
        "executor_memory": conf.get("spark.executor.memory", "default"),
    }


def save_metrics(spark, bucket, run_id, metrics):
    line = json.dumps(metrics)
    print("METRICS_JSON " + line)  # also in the Dataproc job output
    spark.sparkContext.parallelize([line], 1).saveAsTextFile(f"{bucket}/results/metrics/{run_id}")


def main():
    args = parse_args()
    bucket = args.bucket.rstrip("/")
    partition_tag = f"_p{args.partitions}" if args.partitions else ""
    run_id = (f"{args.experiment}_{args.size}_w{args.workers}{partition_tag}"
              f"_r{args.rep}_{uuid.uuid4().hex[:6]}")

    # Step 1 - start Spark (timed separately, not part of the job time)
    start = time.perf_counter()
    spark = SparkSession.builder.appName(f"comp644-{run_id}").getOrCreate()
    startup_seconds = seconds_since(start)

    # Step 2 - define the inputs (lazy: nothing is read yet)
    inputs = load_inputs(spark, bucket, args.size, args.partitions)
    listings, reviews = inputs[0], inputs[1]

    # Step 3 - run and time the 4 stages
    stage_seconds, kmeans_info = run_stages(spark, inputs, f"{bucket}/results/output/{run_id}")

    # Step 4 - record everything the guidelines ask for, then save it
    spark_context = spark.sparkContext
    metrics = {
        "run_id": run_id,
        "timestamp_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "experiment": args.experiment,
        "size_label": args.size,
        "workers": args.workers,
        "rep": args.rep,
        "partitions_setting": args.partitions or "default (file splits)",
        "input_bytes": folder_size_bytes(spark, f"{bucket}/data/size={args.size}"),
        "input_partitions_airbnb": listings.rdd.getNumPartitions(),
        "input_partitions_reviews": reviews.rdd.getNumPartitions(),
        "active_executors": active_executors(spark_context),
        **spark_settings(spark_context),
        "spark_ui": spark_ui_totals(spark_context),
        "spark_version": spark.version,
        "app_id": spark_context.applicationId,
        "startup_s": startup_seconds,
        "stage_s": stage_seconds,
        "job_total_s": round(sum(stage_seconds.values()), 3),
        "kmeans": kmeans_info,
    }
    save_metrics(spark, bucket, run_id, metrics)
    spark.stop()


if __name__ == "__main__":
    main()
