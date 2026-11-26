# make_datasets.py
# One-time data preparation. Run it once before the experiments (it is not timed).
#
# What it does:
#   1. Reads the 2 raw COMP541 files: Airbnb listings and TripAdvisor reviews.
#      Some text fields have line breaks inside them, so Spark must read them with
#      multiLine=true. A multiLine CSV cannot be split, so Spark uses 1 task per file.
#   2. Removes those line breaks and saves a clean "base" copy as ~128 MB files.
#      Now Spark can split the data into many tasks.
#   3. Builds each dataset size (100MB, 500MB, 1GB, 2GB, 5GB, and later 10GB) from the base copy:
#        smaller than the base -> random sample
#        bigger than the base  -> full copies with new IDs (+ a sample for the rest)
#      The mix of Airbnb and review data stays the same at every size.
#   4. Saves a manifest with the real on-disk size, row counts and file counts.
#
# Output format: CSV with a header row, uncompressed.
#
# Example (Dataproc):
#   gcloud dataproc jobs submit pyspark make_datasets.py --cluster=comp644-project \
#       --region=us-central1 -- --bucket=gs://my-bucket --sizes=100MB,500MB,1GB,2GB,5GB
#   Add one more size later, without rebuilding the clean base copy:
#       ... -- --bucket=gs://my-bucket --sizes=10GB --reuse-base

import argparse
import json
import math

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

MB = 1024 ** 2
GB = 1024 ** 3
FILE_SIZE_TARGET = 128 * MB   # Spark reads files in ~128 MB pieces by default
RANDOM_SEED = 42

# Raw file sizes from COMP541. Their ratio (41% / 59%) is kept at every dataset size.
AIRBNB_RAW_MB = 552
REVIEWS_RAW_MB = 808
AIRBNB_SHARE = AIRBNB_RAW_MB / (AIRBNB_RAW_MB + REVIEWS_RAW_MB)
REVIEWS_SHARE = 1 - AIRBNB_SHARE

CSV_OPTIONS = {"header": "true", "quote": '"', "escape": '"'}


def size_label_to_bytes(label):
    # "100MB" -> 100 * 1024^2,  "2GB" -> 2 * 1024^3
    label = label.strip().upper()
    if label.endswith("GB"):
        return float(label[:-2]) * GB
    if label.endswith("MB"):
        return float(label[:-2]) * MB
    raise ValueError(f"Size label must end with MB or GB: {label}")


def read_raw_csv(spark, path, encoding):
    return (spark.read.options(**CSV_OPTIONS)
            .option("multiLine", "true")
            .option("encoding", encoding)
            .csv(path))


def remove_line_breaks(df):
    # Replace line breaks inside every text column with a space
    return df.select([F.regexp_replace(F.col(name), r"[\r\n]+", " ").alias(name)
                      for name in df.columns])


def write_csv(df, path, target_bytes):
    # More data -> more files, each about FILE_SIZE_TARGET
    file_count = max(1, math.ceil(target_bytes / FILE_SIZE_TARGET))
    df.repartition(file_count).write.mode("overwrite").options(**CSV_OPTIONS).csv(path)


def folder_size_bytes(spark, path):
    # Asks the file system (Cloud Storage or local disk) for the total size of a folder
    jvm = spark.sparkContext._jvm
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    folder = jvm.org.apache.hadoop.fs.Path(path)
    return folder.getFileSystem(hadoop_conf).getContentSummary(folder).getLength()


def with_copy_ids(df, copy_name):
    # "123" -> "123_c0", so copied rows are not exact duplicates
    return df.withColumn("id", F.concat_ws("_", F.col("id"), F.lit(copy_name)))


def resize(df, factor):
    # factor 0.5 = half the rows, factor 2.5 = 2 full copies + a 50% sample
    if factor <= 1:
        return df.sample(fraction=factor, seed=RANDOM_SEED)

    full_copies = math.floor(factor)
    result = with_copy_ids(df, "c0")
    for copy_number in range(1, full_copies):
        result = result.unionByName(with_copy_ids(df, f"c{copy_number}"))

    remainder = factor - full_copies
    if remainder > 0.001:
        extra_rows = df.sample(fraction=remainder, seed=RANDOM_SEED)
        result = result.unionByName(with_copy_ids(extra_rows, "cx"))
    return result


def build_base_copy(spark, bucket):
    # Steps 1-2: read the raw files once, remove line breaks, save the clean copy
    airbnb = remove_line_breaks(read_raw_csv(spark, f"{bucket}/raw/airbnb/", "UTF-8"))
    reviews = remove_line_breaks(read_raw_csv(spark, f"{bucket}/raw/tripadvisor/", "ISO-8859-1"))
    write_csv(airbnb, f"{bucket}/data/base/airbnb", AIRBNB_RAW_MB * MB)
    write_csv(reviews, f"{bucket}/data/base/reviews", REVIEWS_RAW_MB * MB)


def describe_dataset(spark, folder):
    airbnb = spark.read.options(**CSV_OPTIONS).csv(f"{folder}/airbnb")
    reviews = spark.read.options(**CSV_OPTIONS).csv(f"{folder}/reviews")
    return {
        "path": folder,
        "format": "CSV, header, uncompressed",
        "bytes": folder_size_bytes(spark, folder),
        "airbnb_rows": airbnb.count(),
        "review_rows": reviews.count(),
        "airbnb_files": len(airbnb.inputFiles()),
        "review_files": len(reviews.inputFiles()),
    }


def read_manifest(spark, path):
    # The manifest is one small JSON text file. Missing (first run) -> start empty.
    try:
        lines = [row.value for row in spark.read.text(path).collect()]
    except Exception:  # the folder does not exist yet
        return {}
    return json.loads("\n".join(lines)) if lines else {}


def build_one_size(spark, bucket, base, label):
    # Step 3: make one dataset size from the clean base copy
    target_bytes = size_label_to_bytes(label)
    folder = f"{bucket}/data/size={label}"
    for source, share in [("airbnb", AIRBNB_SHARE), ("reviews", REVIEWS_SHARE)]:
        factor = target_bytes * share / base[source]["bytes"]
        write_csv(resize(base[source]["df"], factor), f"{folder}/{source}", target_bytes * share)
    return describe_dataset(spark, folder)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--bucket", required=True, help="gs://bucket, or a local folder for testing")
    parser.add_argument("--sizes", default="100MB,500MB,1GB,2GB,5GB", help="comma-separated size labels")
    parser.add_argument("--reuse-base", action="store_true",
                        help="skip steps 1-2 and use the clean base copy that is already in the bucket")
    args = parser.parse_args()
    bucket = args.bucket.rstrip("/")
    labels = [label.strip() for label in args.sizes.split(",") if label.strip()]

    spark = SparkSession.builder.appName("comp644-make-datasets").getOrCreate()
    if not args.reuse_base:
        build_base_copy(spark, bucket)

    # Re-read the clean copy (now splittable) and keep it in memory for all sizes
    base = {}
    for source in ["airbnb", "reviews"]:
        path = f"{bucket}/data/base/{source}"
        base[source] = {"df": spark.read.options(**CSV_OPTIONS).csv(path).cache(),
                        "bytes": folder_size_bytes(spark, path)}

    # Keep the sizes that were built before, and add or replace the ones built now
    manifest_path = f"{bucket}/data/manifest_json"
    manifest = read_manifest(spark, manifest_path)
    for label in labels:
        manifest[label] = build_one_size(spark, bucket, base, label)
        print(label, json.dumps(manifest[label]))

    # Step 4: save the manifest (overwrite is allowed, so a re-run does not fail)
    manifest_text = json.dumps(manifest, indent=2)
    (spark.createDataFrame([(manifest_text,)], ["manifest"])
     .coalesce(1).write.mode("overwrite").text(manifest_path))
    spark.stop()


if __name__ == "__main__":
    main()
