#!/usr/bin/env python3
"""
plot_results.py - Generate pastel plots from sqlbench CSV output.

Usage:
    python plot_results.py --summary results/summary_*.csv --detail results/detail_*.csv [--outdir plots]

Produces PNG files in the output directory for every metric and workload
combination found in the data.  TidesDB is plotted in pastel blue, InnoDB
in pastel orange.
"""

import argparse
import os
import sys

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import pandas as pd
import seaborn as sns

# ---------------------------------------------------------------------------
# Colour palette
# ---------------------------------------------------------------------------
ENGINE_COLORS = {
    "TidesDB": "#003EDC",   # pastel blue
    "InnoDB":  "#E87811",   # pastel orange
}
ENGINE_ORDER = ["InnoDB", "TidesDB"]

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _setup_style():
    sns.set_theme(style="whitegrid", font_scale=1.1)
    plt.rcParams.update({
        "figure.facecolor": "#fafafa",
        "axes.facecolor":   "#fafafa",
        "savefig.dpi":      150,
        "savefig.bbox":     "tight",
    })


def _palette(engines):
    return [ENGINE_COLORS.get(e, "#cccccc") for e in engines]


def _save(fig, outdir, name):
    path = os.path.join(outdir, f"{name}.png")
    fig.savefig(path)
    plt.close(fig)
    print(f"  -> {path}")


def _fmt_title(workload):
    return workload.replace("_", " ").title()


def _thread_label(df):
    """Ensure threads column is treated as a categorical for proper x-axis ordering."""
    df = df.copy()
    df["threads"] = df["threads"].astype(int)
    return df

# ---------------------------------------------------------------------------
# Summary plots
# ---------------------------------------------------------------------------

def plot_summary_bar(df, metric, ylabel, outdir, log_scale=False):
    """Grouped bar chart per workload: x=threads, hue=engine."""
    workloads = sorted(df["workload"].unique())
    for wl in workloads:
        sub = _thread_label(df[df["workload"] == wl])
        engines = [e for e in ENGINE_ORDER if e in sub["engine"].unique()]
        fig, ax = plt.subplots(figsize=(8, 5))
        sns.barplot(
            data=sub, x="threads", y=metric, hue="engine",
            hue_order=engines, palette=_palette(engines),
            edgecolor="white", ax=ax,
        )
        if log_scale and sub[metric].max() / max(sub[metric].min(), 1) > 100:
            ax.set_yscale("log")
            ax.yaxis.set_major_formatter(ticker.ScalarFormatter())
        ax.set_title(f"{_fmt_title(wl)} - {ylabel}")
        ax.set_xlabel("Threads")
        ax.set_ylabel(ylabel)
        ax.legend(title="Engine")
        _save(fig, outdir, f"summary_bar_{metric}_{wl}")


def plot_summary_scatter(df, metric, ylabel, outdir):
    """Scatter plot per workload: x=threads, y=metric, colour=engine."""
    workloads = sorted(df["workload"].unique())
    for wl in workloads:
        sub = _thread_label(df[df["workload"] == wl])
        engines = [e for e in ENGINE_ORDER if e in sub["engine"].unique()]
        fig, ax = plt.subplots(figsize=(8, 5))
        for eng in engines:
            d = sub[sub["engine"] == eng]
            ax.scatter(
                d["threads"], d[metric],
                color=ENGINE_COLORS.get(eng, "#ccc"),
                label=eng, s=90, edgecolors="white", linewidths=0.8, zorder=3,
            )
            ax.plot(
                d.groupby("threads")[metric].mean().index,
                d.groupby("threads")[metric].mean().values,
                color=ENGINE_COLORS.get(eng, "#ccc"),
                linewidth=1.5, alpha=0.6, zorder=2,
            )
        ax.set_title(f"{_fmt_title(wl)} - {ylabel}")
        ax.set_xlabel("Threads")
        ax.set_ylabel(ylabel)
        ax.legend(title="Engine")
        ax.grid(True, alpha=0.3)
        _save(fig, outdir, f"summary_scatter_{metric}_{wl}")


def plot_summary_latency_breakdown(df, outdir):
    """Bar chart showing min / avg / p95 / max latency side by side."""
    lat_cols = ["latency_min_ms", "latency_avg_ms", "latency_p95_ms", "latency_max_ms"]
    labels   = ["Min", "Avg", "P95", "Max"]
    workloads = sorted(df["workload"].unique())
    for wl in workloads:
        sub = _thread_label(df[df["workload"] == wl])
        engines = [e for e in ENGINE_ORDER if e in sub["engine"].unique()]
        thread_vals = sorted(sub["threads"].unique())
        fig, axes = plt.subplots(1, len(thread_vals), figsize=(5 * len(thread_vals), 5), sharey=True)
        if len(thread_vals) == 1:
            axes = [axes]
        for ax, thr in zip(axes, thread_vals):
            tsub = sub[sub["threads"] == thr]
            x_pos = range(len(lat_cols))
            width = 0.35
            for i, eng in enumerate(engines):
                esub = tsub[tsub["engine"] == eng]
                if esub.empty:
                    continue
                vals = [esub[c].values[0] for c in lat_cols]
                offset = -width / 2 + i * width
                ax.bar(
                    [p + offset for p in x_pos], vals, width=width,
                    color=ENGINE_COLORS.get(eng, "#ccc"), label=eng,
                    edgecolor="white",
                )
            ax.set_xticks(list(x_pos))
            ax.set_xticklabels(labels, rotation=45)
            ax.set_title(f"{thr} threads")
            ax.set_ylabel("Latency (ms)" if ax == axes[0] else "")
            ax.legend(title="Engine", fontsize=8)
        fig.suptitle(f"{_fmt_title(wl)} - Latency Breakdown", fontsize=14)
        fig.tight_layout()
        _save(fig, outdir, f"summary_latency_breakdown_{wl}")


def plot_summary_data_size(df, outdir):
    """Bar chart of data size after prepare vs after run."""
    workloads = sorted(df["workload"].unique())
    for wl in workloads:
        sub = _thread_label(df[df["workload"] == wl])
        engines = [e for e in ENGINE_ORDER if e in sub["engine"].unique()]
        thread_vals = sorted(sub["threads"].unique())
        # Only plot for threads=1 to avoid redundancy (prepare size is same across threads)
        tsub = sub[sub["threads"] == thread_vals[0]]
        fig, ax = plt.subplots(figsize=(8, 5))
        x_pos = range(len(engines))
        width = 0.35
        for i, phase in enumerate(["data_size_after_prepare_mb", "data_size_after_run_mb"]):
            label = "After Prepare" if "prepare" in phase else "After Run"
            vals = []
            for eng in engines:
                esub = tsub[tsub["engine"] == eng]
                vals.append(esub[phase].values[0] if not esub.empty else 0)
            offset = -width / 2 + i * width
            ax.bar(
                [p + offset for p in x_pos], vals, width=width,
                label=label, edgecolor="white", alpha=0.85,
            )
        ax.set_xticks(list(x_pos))
        ax.set_xticklabels(engines)
        ax.set_ylabel("Data Size (MB)")
        ax.set_title(f"{_fmt_title(wl)} - Data Size")
        ax.legend()
        _save(fig, outdir, f"summary_datasize_{wl}")


def plot_disk_usage_bar(df, outdir):
    """Grouped bar chart of disk usage (after run) per workload: x=threads, hue=engine."""
    workloads = sorted(df["workload"].unique())
    for wl in workloads:
        sub = _thread_label(df[df["workload"] == wl])
        engines = [e for e in ENGINE_ORDER if e in sub["engine"].unique()]
        fig, ax = plt.subplots(figsize=(8, 5))
        sns.barplot(
            data=sub, x="threads", y="data_size_after_run_mb", hue="engine",
            hue_order=engines, palette=_palette(engines),
            edgecolor="white", ax=ax,
        )
        ax.set_title(f"{_fmt_title(wl)} - Disk Usage After Run")
        ax.set_xlabel("Threads")
        ax.set_ylabel("Data Size (MB)")
        ax.legend(title="Engine")
        _save(fig, outdir, f"summary_disk_usage_bar_{wl}")


def plot_disk_growth_line(df, outdir):
    """Line chart showing disk growth (run - prepare) across threads per workload."""
    df = df.copy()
    df["disk_growth_mb"] = df["data_size_after_run_mb"] - df["data_size_after_prepare_mb"]
    workloads = sorted(df["workload"].unique())
    for wl in workloads:
        sub = _thread_label(df[df["workload"] == wl])
        engines = [e for e in ENGINE_ORDER if e in sub["engine"].unique()]
        fig, ax = plt.subplots(figsize=(8, 5))
        for eng in engines:
            d = sub[sub["engine"] == eng].sort_values("threads")
            ax.plot(
                d["threads"], d["disk_growth_mb"],
                color=ENGINE_COLORS.get(eng, "#ccc"),
                marker="o", linewidth=2, markersize=8,
                label=eng, alpha=0.85,
            )
        ax.set_title(f"{_fmt_title(wl)} - Disk Growth (Run − Prepare)")
        ax.set_xlabel("Threads")
        ax.set_ylabel("Growth (MB)")
        ax.legend(title="Engine")
        ax.grid(True, alpha=0.3)
        _save(fig, outdir, f"summary_disk_growth_line_{wl}")


def plot_disk_stacked_bar(df, outdir):
    """Stacked bar chart showing base size (after prepare) + growth per engine and thread count."""
    import numpy as np
    workloads = sorted(df["workload"].unique())
    for wl in workloads:
        sub = _thread_label(df[df["workload"] == wl])
        engines = [e for e in ENGINE_ORDER if e in sub["engine"].unique()]
        thread_vals = sorted(sub["threads"].unique())
        fig, ax = plt.subplots(figsize=(8, 5))
        n_groups = len(thread_vals)
        n_engines = len(engines)
        bar_width = 0.35
        x = np.arange(n_groups)
        for i, eng in enumerate(engines):
            base_vals = []
            growth_vals = []
            for thr in thread_vals:
                row = sub[(sub["engine"] == eng) & (sub["threads"] == thr)]
                if row.empty:
                    base_vals.append(0)
                    growth_vals.append(0)
                else:
                    prep = row["data_size_after_prepare_mb"].values[0]
                    run = row["data_size_after_run_mb"].values[0]
                    base_vals.append(prep)
                    growth_vals.append(max(run - prep, 0))
            offset = x + (i - (n_engines - 1) / 2) * bar_width
            ax.bar(
                offset, base_vals, bar_width,
                color=ENGINE_COLORS.get(eng, "#ccc"), edgecolor="white",
                label=f"{eng} (base)",
            )
            ax.bar(
                offset, growth_vals, bar_width, bottom=base_vals,
                color=ENGINE_COLORS.get(eng, "#ccc"), edgecolor="white",
                alpha=0.5, label=f"{eng} (growth)",
            )
        ax.set_xticks(x)
        ax.set_xticklabels([str(t) for t in thread_vals])
        ax.set_xlabel("Threads")
        ax.set_ylabel("Data Size (MB)")
        ax.set_title(f"{_fmt_title(wl)} - Disk Usage (Base + Growth)")
        ax.legend(fontsize=8)
        _save(fig, outdir, f"summary_disk_stacked_{wl}")


def generate_summary_plots(csv_path, outdir):
    print(f"\n=== Summary plots from {csv_path} ===")
    df = pd.read_csv(csv_path)

    # Bar + scatter for key metrics
    metrics = [
        ("tps",              "Transactions / sec",   True),
        ("qps",              "Queries / sec",         True),
        ("reads_per_sec",    "Reads / sec",           True),
        ("writes_per_sec",   "Writes / sec",          True),
        ("latency_avg_ms",   "Avg Latency (ms)",      False),
        ("latency_p95_ms",   "P95 Latency (ms)",      False),
        ("latency_max_ms",   "Max Latency (ms)",      False),
    ]
    for metric, ylabel, log_scale in metrics:
        # Skip metrics that are all zero
        if df[metric].sum() == 0:
            continue
        plot_summary_bar(df, metric, ylabel, outdir, log_scale=log_scale)
        plot_summary_scatter(df, metric, ylabel, outdir)

    plot_summary_latency_breakdown(df, outdir)
    plot_summary_data_size(df, outdir)
    plot_disk_usage_bar(df, outdir)
    plot_disk_growth_line(df, outdir)
    plot_disk_stacked_bar(df, outdir)


# ---------------------------------------------------------------------------
# Detail (interval) plots
# ---------------------------------------------------------------------------

def plot_detail_timeseries(df, metric, ylabel, outdir):
    """Time-series line plot per workload+threads: x=time_s, y=metric."""
    workloads = sorted(df["workload"].unique())
    for wl in workloads:
        wl_df = df[df["workload"] == wl]
        thread_vals = sorted(wl_df["threads"].unique())
        fig, axes = plt.subplots(
            1, len(thread_vals),
            figsize=(6 * len(thread_vals), 5),
            sharey=True,
        )
        if len(thread_vals) == 1:
            axes = [axes]
        for ax, thr in zip(axes, thread_vals):
            tsub = wl_df[wl_df["threads"] == thr]
            engines = [e for e in ENGINE_ORDER if e in tsub["engine"].unique()]
            for eng in engines:
                esub = tsub[tsub["engine"] == eng].sort_values("time_s")
                ax.plot(
                    esub["time_s"], esub[metric],
                    color=ENGINE_COLORS.get(eng, "#ccc"),
                    label=eng, linewidth=1.5, alpha=0.85,
                )
            ax.set_title(f"{thr} threads")
            ax.set_xlabel("Time (s)")
            ax.set_ylabel(ylabel if ax == axes[0] else "")
            ax.legend(title="Engine", fontsize=8)
            ax.grid(True, alpha=0.3)
        fig.suptitle(f"{_fmt_title(wl)} - {ylabel} Over Time", fontsize=14)
        fig.tight_layout()
        _save(fig, outdir, f"detail_ts_{metric}_{wl}")


def plot_detail_scatter(df, metric, ylabel, outdir):
    """Scatter of all interval samples: x=time_s, y=metric, per workload+threads."""
    workloads = sorted(df["workload"].unique())
    for wl in workloads:
        wl_df = df[df["workload"] == wl]
        thread_vals = sorted(wl_df["threads"].unique())
        fig, axes = plt.subplots(
            1, len(thread_vals),
            figsize=(6 * len(thread_vals), 5),
            sharey=True,
        )
        if len(thread_vals) == 1:
            axes = [axes]
        for ax, thr in zip(axes, thread_vals):
            tsub = wl_df[wl_df["threads"] == thr]
            engines = [e for e in ENGINE_ORDER if e in tsub["engine"].unique()]
            for eng in engines:
                esub = tsub[tsub["engine"] == eng]
                ax.scatter(
                    esub["time_s"], esub[metric],
                    color=ENGINE_COLORS.get(eng, "#ccc"),
                    label=eng, s=30, alpha=0.7, edgecolors="white", linewidths=0.5,
                )
            ax.set_title(f"{thr} threads")
            ax.set_xlabel("Time (s)")
            ax.set_ylabel(ylabel if ax == axes[0] else "")
            ax.legend(title="Engine", fontsize=8)
            ax.grid(True, alpha=0.3)
        fig.suptitle(f"{_fmt_title(wl)} - {ylabel} Scatter", fontsize=14)
        fig.tight_layout()
        _save(fig, outdir, f"detail_scatter_{metric}_{wl}")


def generate_detail_plots(csv_path, outdir):
    print(f"\n=== Detail plots from {csv_path} ===")
    df = pd.read_csv(csv_path)

    metrics = [
        ("tps",             "TPS"),
        ("qps",             "QPS"),
        ("latency_avg_ms",  "Avg Latency (ms)"),
        ("latency_p95_ms",  "P95 Latency (ms)"),
    ]
    for metric, ylabel in metrics:
        if metric not in df.columns or df[metric].sum() == 0:
            continue
        plot_detail_timeseries(df, metric, ylabel, outdir)
        plot_detail_scatter(df, metric, ylabel, outdir)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Generate plots from sqlbench CSV results")
    parser.add_argument("--summary", required=True, help="Path to summary CSV")
    parser.add_argument("--detail",  required=True, help="Path to detail CSV")
    parser.add_argument("--outdir",  default="plots", help="Output directory for PNGs (default: plots)")
    args = parser.parse_args()

    for path in [args.summary, args.detail]:
        if not os.path.isfile(path):
            print(f"ERROR: File not found: {path}", file=sys.stderr)
            sys.exit(1)

    os.makedirs(args.outdir, exist_ok=True)
    _setup_style()

    generate_summary_plots(args.summary, args.outdir)
    generate_detail_plots(args.detail, args.outdir)

    print(f"\nDone. All plots saved to {args.outdir}/")


if __name__ == "__main__":
    main()
