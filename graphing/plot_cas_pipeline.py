from __future__ import annotations

import argparse
import csv
import math
from collections import defaultdict
from pathlib import Path
from statistics import median

try:
    import matplotlib.pyplot as plt
except ModuleNotFoundError as exc:
    raise SystemExit(
        "matplotlib is required. Install it with: py -3 -m pip install matplotlib"
    ) from exc


NUMERIC_FIELDS = {
    "client_machine_id": int,
    "clients": int,
    "locks": int,
    "active_window": int,
    "zipf_skew": float,
    "total_ops": int,
    "wall_s": float,
    "goodput": float,
    "mean_us": float,
    "p50_us": float,
    "p90_us": float,
    "p99_us": float,
    "p99.9_us": float,
    "max_us": float,
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Plot CAS pipeline benchmark results")
    parser.add_argument(
        "--input",
        default="graphing/data/cas_pipelined.csv",
        help="Input CSV path",
    )
    parser.add_argument(
        "--outdir",
        default="graphing/plots",
        help="Directory for generated plots",
    )
    parser.add_argument(
        "--skew",
        type=float,
        default=0.0,
        help="Zipf skew value to plot",
    )
    return parser.parse_args()


def load_rows(csv_path: Path) -> list[dict]:
    rows: list[dict] = []
    with csv_path.open(newline="") as handle:
        for raw in csv.DictReader(handle):
            if not raw.get("strategy", "").strip():
                continue

            row = dict(raw)
            for field, caster in NUMERIC_FIELDS.items():
                row[field] = caster(raw[field])
            rows.append(row)
    return rows


def aggregate_rows(rows: list[dict], skew: float) -> list[dict]:
    grouped: dict[tuple[int, int, float], list[dict]] = defaultdict(list)
    for row in rows:
        if row["strategy"] != "cas":
            continue
        if not math.isclose(row["zipf_skew"], skew, rel_tol=0.0, abs_tol=1e-9):
            continue
        grouped[(row["locks"], row["active_window"], row["zipf_skew"])].append(row)

    aggregated: list[dict] = []
    for (locks, window, row_skew), group in grouped.items():
        merged = {
            "strategy": "cas",
            "locks": locks,
            "active_window": window,
            "zipf_skew": row_skew,
        }
        for field in (
            "total_ops",
            "wall_s",
            "goodput",
            "mean_us",
            "p50_us",
            "p90_us",
            "p99_us",
            "p99.9_us",
            "max_us",
        ):
            merged[field] = median(row[field] for row in group)
        aggregated.append(merged)

    aggregated.sort(key=lambda row: (row["locks"], row["active_window"]))
    return aggregated


def by_locks(rows: list[dict]) -> dict[int, list[dict]]:
    grouped: dict[int, list[dict]] = defaultdict(list)
    for row in rows:
        grouped[row["locks"]].append(row)
    for group in grouped.values():
        group.sort(key=lambda row: row["active_window"])
    return dict(sorted(grouped.items()))


def best_rows(rows: list[dict]) -> list[dict]:
    grouped = by_locks(rows)
    best: list[dict] = []
    for _, group in grouped.items():
        best.append(max(group, key=lambda row: row["goodput"]))
    return best


def finish_plot(fig, out_path: Path) -> None:
    fig.tight_layout()
    fig.savefig(out_path, dpi=180)
    plt.close(fig)


def plot_goodput_vs_window(rows: list[dict], outdir: Path, skew: float) -> None:
    fig, ax = plt.subplots(figsize=(9, 5.5))
    for locks, group in by_locks(rows).items():
        xs = [row["active_window"] for row in group]
        ys = [row["goodput"] for row in group]
        ax.plot(xs, ys, marker="o", linewidth=2, label=f"locks={locks}")

        best = max(group, key=lambda row: row["goodput"])
        ax.annotate(
            f"{int(best['goodput']):,}",
            (best["active_window"], best["goodput"]),
            textcoords="offset points",
            xytext=(0, 8),
            ha="center",
            fontsize=8,
        )

    ax.set_title(f"CAS Goodput vs Active Window (skew={skew:.2f})")
    ax.set_xlabel("Active window")
    ax.set_ylabel("Goodput (ops/s)")
    ax.set_xscale("log", base=2)
    ax.grid(True, alpha=0.3)
    ax.legend()
    finish_plot(fig, outdir / f"goodput_vs_window_skew_{skew:.2f}.png")


def plot_tail_latency_vs_window(rows: list[dict], outdir: Path, skew: float) -> None:
    fig, axes = plt.subplots(1, 2, figsize=(12, 5.5), sharex=True)
    metrics = [("p99_us", "P99 (us)"), ("p99.9_us", "P99.9 (us)")]

    grouped = by_locks(rows)
    for axis, (metric, ylabel) in zip(axes, metrics, strict=True):
        for locks, group in grouped.items():
            xs = [row["active_window"] for row in group]
            ys = [row[metric] for row in group]
            axis.plot(xs, ys, marker="o", linewidth=2, label=f"locks={locks}")
        axis.set_xscale("log", base=2)
        axis.set_xlabel("Active window")
        axis.set_ylabel(ylabel)
        axis.grid(True, alpha=0.3)

    axes[0].set_title(f"CAS Tail Latency vs Active Window (skew={skew:.2f})")
    axes[1].legend()
    finish_plot(fig, outdir / f"tail_latency_vs_window_skew_{skew:.2f}.png")


def plot_best_goodput_vs_locks(rows: list[dict], outdir: Path, skew: float) -> None:
    best = best_rows(rows)
    fig, ax = plt.subplots(figsize=(8, 5.5))

    xs = [row["locks"] for row in best]
    ys = [row["goodput"] for row in best]
    ax.plot(xs, ys, marker="o", linewidth=2)

    for row in best:
        ax.annotate(
            f"w={row['active_window']}",
            (row["locks"], row["goodput"]),
            textcoords="offset points",
            xytext=(0, 8),
            ha="center",
            fontsize=8,
        )

    ax.set_title(f"Best CAS Goodput by Lock Count (skew={skew:.2f})")
    ax.set_xlabel("Locks")
    ax.set_ylabel("Best goodput (ops/s)")
    ax.set_xscale("log", base=10)
    ax.grid(True, alpha=0.3)
    finish_plot(fig, outdir / f"best_goodput_vs_locks_skew_{skew:.2f}.png")


def plot_best_window_vs_locks(rows: list[dict], outdir: Path, skew: float) -> None:
    best = best_rows(rows)
    fig, ax = plt.subplots(figsize=(8, 5.5))

    xs = [row["locks"] for row in best]
    ys = [row["active_window"] for row in best]
    ax.plot(xs, ys, marker="o", linewidth=2)

    for row in best:
        ax.annotate(
            f"{int(row['goodput']):,}",
            (row["locks"], row["active_window"]),
            textcoords="offset points",
            xytext=(0, 8),
            ha="center",
            fontsize=8,
        )

    ax.set_title(f"Best Active Window by Lock Count (skew={skew:.2f})")
    ax.set_xlabel("Locks")
    ax.set_ylabel("Best active window")
    ax.set_xscale("log", base=10)
    ax.set_yscale("log", base=2)
    ax.grid(True, alpha=0.3)
    finish_plot(fig, outdir / f"best_window_vs_locks_skew_{skew:.2f}.png")


def plot_goodput_heatmap(rows: list[dict], outdir: Path, skew: float) -> None:
    locks = sorted({row["locks"] for row in rows})
    windows = sorted({row["active_window"] for row in rows})
    lookup = {(row["locks"], row["active_window"]): row["goodput"] for row in rows}

    heatmap = []
    for lock in locks:
        heatmap.append([lookup.get((lock, window), math.nan) for window in windows])

    fig, ax = plt.subplots(figsize=(9, 5.5))
    image = ax.imshow(heatmap, aspect="auto", cmap="viridis")

    ax.set_title(f"CAS Goodput Heatmap (skew={skew:.2f})")
    ax.set_xlabel("Active window")
    ax.set_ylabel("Locks")
    ax.set_xticks(range(len(windows)), labels=[str(window) for window in windows])
    ax.set_yticks(range(len(locks)), labels=[str(lock) for lock in locks])

    cbar = fig.colorbar(image, ax=ax)
    cbar.set_label("Goodput (ops/s)")

    finish_plot(fig, outdir / f"goodput_heatmap_skew_{skew:.2f}.png")


def write_summary(rows: list[dict], outdir: Path, skew: float) -> None:
    lines = [f"CAS benchmark summary for skew={skew:.2f}", ""]
    for row in best_rows(rows):
        lines.append(
            f"locks={row['locks']}: best_window={row['active_window']}, "
            f"goodput={int(row['goodput']):,} ops/s, "
            f"p99={row['p99_us']:.2f} us, p99.9={row['p99.9_us']:.2f} us"
        )
    (outdir / f"summary_skew_{skew:.2f}.txt").write_text("\n".join(lines) + "\n")


def main() -> None:
    args = parse_args()
    csv_path = Path(args.input)
    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    rows = load_rows(csv_path)
    aggregated = aggregate_rows(rows, args.skew)
    if not aggregated:
        raise SystemExit(f"No CAS rows found for skew={args.skew:.2f} in {csv_path}")

    plot_goodput_vs_window(aggregated, outdir, args.skew)
    plot_tail_latency_vs_window(aggregated, outdir, args.skew)
    plot_best_goodput_vs_locks(aggregated, outdir, args.skew)
    plot_best_window_vs_locks(aggregated, outdir, args.skew)
    plot_goodput_heatmap(aggregated, outdir, args.skew)
    write_summary(aggregated, outdir, args.skew)

    print(f"Loaded {len(rows)} nonblank CSV rows")
    print(f"Generated plots in {outdir}")


if __name__ == "__main__":
    main()
