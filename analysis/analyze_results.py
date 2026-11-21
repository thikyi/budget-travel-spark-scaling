# analyze_results.py
# Turns the METRICS_JSON lines from all runs into the tables and graphs for the report.
#
# Input:  metrics.jsonl (one JSON line per run, made by gcp/collect_results.sh)
# Output: results/ folder with
#   raw_timing_table.csv      every run (all 3 repeats)
#   expA_summary.csv          Experiment A, median of the repeats
#   expB_summary.csv          Experiment B, median of the repeats
#   expC_summary.csv          Experiment C, partition study (if it was run)
#   config_cost_table.csv     cluster sizes and estimated cost per hour
#   summary.md                all summary tables in one file
#   graph1..graph5 *.png      the 5 required graphs, plus graph6 (Experiment C) and 2 extra graphs
#
# Formulas (from the project guidelines):
#   throughput (MB/s)        = input MB / seconds
#   speedup(N)               = T1 / TN, where T1 = median time with 1 worker
#   relative efficiency(N)   = speedup(N) / N x 100%
#   cost per run ($)         = (master $/h + N x worker $/h) x TN / 3600
#
# Example:
#   python analyze_results.py metrics.jsonl --expA-workers 2 --expB-size 2GB --expC-size 2GB

import argparse
import json
from pathlib import Path

import matplotlib

matplotlib.use("Agg")  # draw to files, no screen needed
import matplotlib.pyplot as plt  # noqa: E402
import pandas as pd  # noqa: E402

WORKER_VCPUS = 2       # n1-standard-2
MASTER_VCPUS = 4       # n4-standard-4
MB_PER_GB = 1024

# Chart colors and text colors
BLUE, ORANGE, GREEN, YELLOW = "#2a78d6", "#eb6834", "#1baf7a", "#eda100"
TEXT_DARK, TEXT_GRAY, GRID_GRAY = "#0b0b0b", "#52514e", "#e4e3df"

STAGE_NAMES = {"s1_lodging": "S1 Airbnb aggregation", "s2_sentiment": "S2 review sentiment",
               "s3_kmeans": "S3 K-Means", "s4_ranking": "S4 ranking + joins"}


# Loading the runs

def size_label_to_mb(label):
    # "2GB" -> 2048, "100MB" -> 100
    label = str(label).strip().upper()
    if label.endswith("GB"):
        return float(label[:-2]) * MB_PER_GB
    return float(label.replace("MB", ""))


def load_runs(metrics_file):
    lines = Path(metrics_file).read_text().splitlines()
    runs = [json.loads(line.replace("METRICS_JSON ", "")) for line in lines if line.strip()]
    df = pd.json_normalize(runs)  # nested fields become columns like "stage_s.s1_lodging"
    df["time_s"] = df["job_total_s"]
    df["input_mb"] = df["input_bytes"] / 1024 ** 2
    df["size_mb"] = df["size_label"].map(size_label_to_mb)
    # Experiment C runs store a number here; normal runs store the text "default (file splits)"
    df["expc_partitions"] = pd.to_numeric(df["partitions_setting"], errors="coerce")
    return df


def median_by(runs, column):
    # One row per group: median time (what we report) plus min, max and the run count
    groups = runs.groupby(column)
    summary = groups["time_s"].agg(runs="size", median_s="median", min_s="min", max_s="max")
    summary["input_mb"] = groups["input_mb"].median()
    for stage in STAGE_NAMES:
        summary[stage] = groups[f"stage_s.{stage}"].median()
    extra_columns = {"spark_ui.tasks": "tasks", "spark_ui.shuffle_write_mb": "shuffle_write_mb",
                     "spark_ui.disk_spill_mb": "disk_spill_mb", "active_executors": "active_executors",
                     "input_partitions_airbnb": "input_partitions_airbnb",
                     "input_partitions_reviews": "input_partitions_reviews"}
    for source, name in extra_columns.items():
        if source in runs:
            summary[name] = groups[source].median()
    return summary.reset_index()


# Drawing the graphs

def new_chart(title, x_label, y_label):
    fig, ax = plt.subplots(figsize=(7, 4.2))
    ax.set_title(title, loc="left", fontsize=12, color=TEXT_DARK, pad=10)
    ax.set_xlabel(x_label, color=TEXT_GRAY)
    ax.set_ylabel(y_label, color=TEXT_GRAY)
    ax.grid(axis="y", color=GRID_GRAY, linewidth=0.8)
    ax.set_axisbelow(True)
    for side in ["top", "right"]:
        ax.spines[side].set_visible(False)
    for side in ["left", "bottom"]:
        ax.spines[side].set_color(GRID_GRAY)
    ax.tick_params(colors=TEXT_GRAY)
    return fig, ax


def plot_measured(ax, x, y):
    ax.plot(x, y, color=BLUE, linewidth=2, marker="o", markersize=8, label="measured (median of runs)")


def plot_reference(ax, x, y, label):
    ax.plot(x, y, color=TEXT_GRAY, linewidth=1.5, linestyle="--", label=label)
    ax.legend(frameon=False)


def save_chart(fig, path):
    fig.tight_layout()
    fig.savefig(path, dpi=150)
    plt.close(fig)


def stage_bars_by_size(summary, title, path):
    # Stacked bars: how the time splits over the 4 stages, for each dataset size
    fig, ax = new_chart(title, "dataset size", "seconds (median)")
    labels = summary["size_label"].astype(str)
    bottom = 0
    for color, (stage, name) in zip([BLUE, ORANGE, GREEN, YELLOW], STAGE_NAMES.items()):
        ax.bar(labels, summary[stage], bottom=bottom, color=color, width=0.55,
               edgecolor="white", linewidth=2, label=name)
        bottom = bottom + summary[stage]
    handles, names = ax.get_legend_handles_labels()
    ax.legend(handles[::-1], names[::-1], frameon=False, fontsize=9)  # same order as the stack
    save_chart(fig, path)


# Experiment A: data grows, cluster fixed

def experiment_a(runs, workers, out_dir):
    normal_runs = runs[(runs["workers"] == workers) & runs["expc_partitions"].isna()]
    summary = median_by(normal_runs, "size_label")
    summary["size_mb"] = summary["size_label"].map(size_label_to_mb)
    summary = summary.sort_values("size_mb").reset_index(drop=True)

    smallest = summary.iloc[0]
    summary["throughput_mb_s"] = summary["input_mb"] / summary["median_s"]
    summary["data_ratio"] = summary["input_mb"] / smallest["input_mb"]
    summary["time_ratio"] = summary["median_s"] / smallest["median_s"]
    summary.to_csv(out_dir / "expA_summary.csv", index=False)

    fig, ax = new_chart(f"Graph 1 - Input size vs execution time ({workers} workers)",
                        "input size on disk (MB)", "execution time (s)")
    plot_measured(ax, summary["input_mb"], summary["median_s"])
    plot_reference(ax, summary["input_mb"], smallest["median_s"] * summary["data_ratio"],
                   "perfectly linear (from the smallest size)")
    save_chart(fig, out_dir / "graph1_expA_size_vs_time.png")

    fig, ax = new_chart(f"Graph 2 - Input size vs throughput ({workers} workers)",
                        "input size on disk (MB)", "throughput (MB/s)")
    plot_measured(ax, summary["input_mb"], summary["throughput_mb_s"])
    ax.set_ylim(bottom=0)
    save_chart(fig, out_dir / "graph2_expA_size_vs_throughput.png")

    stage_bars_by_size(summary, f"Extra - time per stage ({workers} workers)",
                       out_dir / "extra_expA_stage_breakdown.png")
    return summary


# Experiment B: workers grow, data fixed

def experiment_b(runs, size, prices, out_dir):
    normal_runs = runs[(runs["size_label"] == size) & runs["expc_partitions"].isna()]
    summary = median_by(normal_runs, "workers").sort_values("workers").reset_index(drop=True)

    baseline = summary.iloc[0]  # should be the 1-worker run
    if baseline["workers"] != 1:
        print(f"Warning: no 1-worker run yet, using {int(baseline['workers'])} workers as the baseline.")

    summary["worker_vcpus"] = summary["workers"] * WORKER_VCPUS
    summary["speedup"] = baseline["median_s"] / summary["median_s"]
    summary["ideal_speedup"] = summary["workers"] / baseline["workers"]
    summary["relative_efficiency_pct"] = summary["speedup"] / summary["ideal_speedup"] * 100
    summary["cluster_cost_per_hour"] = prices["master"] + summary["workers"] * prices["worker"]
    summary["cost_per_run_usd"] = summary["cluster_cost_per_hour"] * summary["median_s"] / 3600
    summary.to_csv(out_dir / "expB_summary.csv", index=False)

    worker_counts = list(summary["workers"])
    graphs = [
        ("graph3_expB_workers_vs_time.png", f"Graph 3 - Workers vs execution time ({size})",
         "execution time (s)", "median_s", baseline["median_s"] / summary["ideal_speedup"], "ideal (time / N)"),
        ("graph4_expB_workers_vs_speedup.png", f"Graph 4 - Workers vs speedup ({size}, baseline = 1 worker)",
         "speedup T1 / TN", "speedup", summary["ideal_speedup"], "ideal (linear)"),
        ("graph5_expB_workers_vs_efficiency.png", f"Graph 5 - Workers vs relative efficiency ({size})",
         "relative efficiency (%)", "relative_efficiency_pct", [100] * len(summary), "100% = perfect scaling"),
    ]
    for file_name, title, y_label, column, reference, reference_label in graphs:
        fig, ax = new_chart(title, "worker nodes", y_label)
        plot_measured(ax, summary["workers"], summary[column])
        plot_reference(ax, summary["workers"], reference, reference_label)
        ax.set_xticks(worker_counts)
        ax.set_ylim(bottom=0)
        save_chart(fig, out_dir / file_name)

    cost_bars(summary, size, out_dir / "extra_expB_cost_per_run.png")
    return summary


def cost_bars(summary, size, path):
    fig, ax = new_chart(f"Extra - estimated cost per run ({size})", "worker nodes", "USD per run")
    bars = ax.bar(summary["workers"].astype(int).astype(str), summary["cost_per_run_usd"], color=BLUE, width=0.5)
    for bar, cost, seconds in zip(bars, summary["cost_per_run_usd"], summary["median_s"]):
        ax.annotate(f"${cost:.3f}\n{seconds:.0f} s", (bar.get_x() + bar.get_width() / 2, bar.get_height()),
                    ha="center", va="bottom", fontsize=9, color=TEXT_DARK,
                    xytext=(0, 3), textcoords="offset points")
    ax.margins(y=0.2)
    save_chart(fig, path)


# Experiment C (the optional bonus): partitions change, everything else fixed

def experiment_c(runs, size, out_dir):
    expc_runs = runs[(runs["size_label"] == size) & runs["expc_partitions"].notna()]
    if expc_runs.empty:
        return None

    summary = median_by(expc_runs, "expc_partitions").rename(columns={"expc_partitions": "partitions"})
    summary = summary.sort_values("partitions").reset_index(drop=True)
    summary.to_csv(out_dir / "expC_summary.csv", index=False)

    best = summary.loc[summary["median_s"].idxmin(), "partitions"]
    fig, ax = new_chart(f"Experiment C - Number of partitions vs execution time ({size}; best = {int(best)})",
                        "number of partitions (repartition N)", "execution time (s)")
    plot_measured(ax, summary["partitions"], summary["median_s"])
    ax.set_xscale("log", base=2)  # 4, 8, 16, 32, 64 are evenly spaced on a log-2 axis
    ax.set_xticks(summary["partitions"])
    ax.set_xticklabels([str(int(n)) for n in summary["partitions"]])
    ax.set_ylim(bottom=0)
    save_chart(fig, out_dir / "graph6_expC_partitions_vs_time.png")
    return summary


# Tables for the report

def raw_timing_table(runs, out_dir):
    columns = ["run_id", "experiment", "size_label", "workers", "partitions_setting", "rep", "time_s",
               "stage_s.s1_lodging", "stage_s.s2_sentiment", "stage_s.s3_kmeans", "stage_s.s4_ranking",
               "startup_s", "input_mb", "input_partitions_airbnb", "input_partitions_reviews",
               "active_executors", "spark_ui.tasks", "spark_ui.shuffle_write_mb",
               "spark_ui.memory_spill_mb", "spark_ui.disk_spill_mb", "executor_cores",
               "executor_memory", "spark_version", "timestamp_utc"]
    table = runs[[name for name in columns if name in runs.columns]]
    table = table.sort_values(["experiment", "size_label", "workers", "rep"])
    table.to_csv(out_dir / "raw_timing_table.csv", index=False)
    return table


def config_cost_table(runs, prices, out_dir):
    table = pd.DataFrame({"workers": sorted(runs["workers"].unique())})
    table["master"] = "n4-standard-4 (4 vCPU, 16 GiB)"
    table["worker_type"] = "n1-standard-2 (2 vCPU, 7.5 GiB)"
    table["worker_vcpus"] = table["workers"] * WORKER_VCPUS
    table["total_vcpus"] = table["worker_vcpus"] + MASTER_VCPUS
    table["est_cost_per_hour_usd"] = prices["master"] + table["workers"] * prices["worker"]
    table.to_csv(out_dir / "config_cost_table.csv", index=False)
    return table


def write_summary(out_dir, run_count, tables):
    lines = ["# Results summary", "",
             f"Runs analysed: {run_count} (warm-up and smoke runs excluded). Times = median of the repeats.", ""]
    for title, table, columns, note in tables:
        if table is None:
            continue
        lines += [f"## {title}", "", table[columns].round(3).to_markdown(index=False), "", note, ""]
    text = "\n".join(lines)
    (out_dir / "summary.md").write_text(text)
    print(text)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("metrics", help="metrics.jsonl")
    parser.add_argument("--expA-workers", type=int, default=2)
    parser.add_argument("--expB-size", default="2GB")
    parser.add_argument("--expC-size", default="2GB")
    # Approximate prices incl. the Dataproc fee - check the Google Cloud pricing calculator
    parser.add_argument("--master-cost", type=float, default=0.23, help="$/hour, n4-standard-4")
    parser.add_argument("--worker-cost", type=float, default=0.115, help="$/hour, n1-standard-2")
    parser.add_argument("--out", default="results")
    args = parser.parse_args()

    out_dir = Path(args.out)
    out_dir.mkdir(exist_ok=True)
    prices = {"master": args.master_cost, "worker": args.worker_cost}

    runs = load_runs(args.metrics)
    raw_timing_table(runs, out_dir)
    exp_a = experiment_a(runs, args.expA_workers, out_dir)
    exp_b = experiment_b(runs, args.expB_size, prices, out_dir)
    exp_c = experiment_c(runs, args.expC_size, out_dir)
    config_table = config_cost_table(runs, prices, out_dir)

    write_summary(out_dir, len(runs), [
        (f"Experiment A - input data scalability ({args.expA_workers} workers)", exp_a,
         ["size_label", "input_mb", "runs", "median_s", "min_s", "max_s", "throughput_mb_s",
          "data_ratio", "time_ratio"],
         "If time_ratio < data_ratio, time grows slower than the data (fixed costs are spread out)."),
        (f"Experiment B - worker scalability ({args.expB_size})", exp_b,
         ["workers", "worker_vcpus", "runs", "median_s", "min_s", "max_s", "speedup",
          "ideal_speedup", "relative_efficiency_pct", "cost_per_run_usd"],
         "Speedup and efficiency use the 1-worker median as the baseline (T1)."),
        (f"Experiment C - partition scalability ({args.expC_size}, {args.expA_workers} workers)", exp_c,
         ["partitions", "runs", "median_s", "min_s", "max_s"],
         "The time includes the repartition(N) shuffle."),
        ("Configuration and cost", config_table,
         ["workers", "master", "worker_type", "worker_vcpus", "total_vcpus", "est_cost_per_hour_usd"],
         "Prices are estimates; check the Google Cloud pricing calculator."),
    ])
    print(f"\nTables and graphs saved in: {out_dir.resolve()}")


if __name__ == "__main__":
    main()
