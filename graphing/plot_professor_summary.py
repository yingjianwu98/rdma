from __future__ import annotations

import csv
import math
from collections import defaultdict
from pathlib import Path
from statistics import median

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt


CSV_PATH = Path("graphing/data/cas_pipelined.csv")
OUTDIR = Path("graphing/plots/professor")
SKEW = 0.0

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


def load_rows() -> list[dict]:
    rows: list[dict] = []
    with CSV_PATH.open(newline="") as handle:
        for raw in csv.DictReader(handle):
            if not raw.get("strategy", "").strip():
                continue

            row = dict(raw)
            for field, caster in NUMERIC_FIELDS.items():
                row[field] = caster(raw[field])
            rows.append(row)
    return rows


def aggregate_rows(rows: list[dict]) -> list[dict]:
    grouped: dict[tuple[int, int, int, float], list[dict]] = defaultdict(list)
    for row in rows:
        if row["strategy"] != "cas":
            continue
        if not math.isclose(row["zipf_skew"], SKEW, rel_tol=0.0, abs_tol=1e-9):
            continue
        key = (row["clients"], row["locks"], row["active_window"], row["zipf_skew"])
        grouped[key].append(row)

    aggregated: list[dict] = []
    for (clients, locks, active_window, zipf_skew), group in grouped.items():
        merged = {
            "strategy": "cas",
            "clients": clients,
            "locks": locks,
            "active_window": active_window,
            "zipf_skew": zipf_skew,
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

    aggregated.sort(key=lambda row: (row["clients"], row["locks"], row["active_window"]))
    return aggregated


def finish_plot(fig: plt.Figure, path: Path) -> None:
    fig.tight_layout()
    fig.savefig(path, dpi=180, bbox_inches="tight")
    plt.close(fig)


def group_rows(rows: list[dict], key: str) -> dict[int, list[dict]]:
    grouped: dict[int, list[dict]] = defaultdict(list)
    for row in rows:
        grouped[row[key]].append(row)
    for group in grouped.values():
        group.sort(key=lambda row: row["active_window"] if key != "clients" else row["clients"])
    return dict(sorted(grouped.items()))


def best_rows_by_lock(rows: list[dict]) -> list[dict]:
    grouped = group_rows(rows, "locks")
    return [max(group, key=lambda row: row["goodput"]) for group in grouped.values()]


def plot_single_client_goodput(rows: list[dict], outdir: Path) -> None:
    fig, ax = plt.subplots(figsize=(9, 5.5))
    for locks, group in group_rows(rows, "locks").items():
        xs = [row["active_window"] for row in group]
        ys = [row["goodput"] for row in group]
        ax.plot(xs, ys, marker="o", linewidth=2, label=f"locks={locks}")

    ax.set_title("Single-Client CAS Goodput vs Active Window")
    ax.set_xlabel("Active window")
    ax.set_ylabel("Goodput (ops/s)")
    ax.set_xscale("log", base=2)
    ax.grid(True, alpha=0.3)
    ax.legend(ncols=2)
    finish_plot(fig, outdir / "single_client_goodput_vs_window_core.png")


def plot_single_client_goodput_large(rows: list[dict], outdir: Path) -> None:
    fig, ax = plt.subplots(figsize=(9, 5.5))
    for locks, group in group_rows(rows, "locks").items():
        xs = [row["active_window"] for row in group]
        ys = [row["goodput"] for row in group]
        ax.plot(xs, ys, marker="o", linewidth=2, label=f"locks={locks}")

    ax.set_title("Single-Client CAS Goodput vs Active Window (Large Lock Sets)")
    ax.set_xlabel("Active window")
    ax.set_ylabel("Goodput (ops/s)")
    ax.set_xscale("log", base=2)
    ax.grid(True, alpha=0.3)
    ax.legend()
    finish_plot(fig, outdir / "single_client_goodput_vs_window_large_locks.png")


def plot_single_client_p99(rows: list[dict], outdir: Path) -> None:
    fig, ax = plt.subplots(figsize=(9, 5.5))
    for locks, group in group_rows(rows, "locks").items():
        xs = [row["active_window"] for row in group]
        ys = [row["p99_us"] for row in group]
        ax.plot(xs, ys, marker="o", linewidth=2, label=f"locks={locks}")

    ax.set_title("Single-Client CAS P99 vs Active Window")
    ax.set_xlabel("Active window")
    ax.set_ylabel("P99 latency (us)")
    ax.set_xscale("log", base=2)
    ax.grid(True, alpha=0.3)
    ax.legend(ncols=2)
    finish_plot(fig, outdir / "single_client_p99_vs_window_core.png")


def plot_best_goodput_vs_locks(rows: list[dict], outdir: Path) -> None:
    best = best_rows_by_lock(rows)
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

    ax.set_title("Best Single-Client CAS Goodput by Lock Count")
    ax.set_xlabel("Locks")
    ax.set_ylabel("Best goodput (ops/s)")
    ax.set_xscale("log", base=10)
    ax.grid(True, alpha=0.3)
    finish_plot(fig, outdir / "best_goodput_vs_locks.png")


def plot_best_window_vs_locks(rows: list[dict], outdir: Path) -> None:
    best = best_rows_by_lock(rows)
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

    ax.set_title("Best Active Window by Lock Count")
    ax.set_xlabel("Locks")
    ax.set_ylabel("Best active window")
    ax.set_xscale("log", base=10)
    ax.set_yscale("log", base=2)
    ax.grid(True, alpha=0.3)
    finish_plot(fig, outdir / "best_window_vs_locks.png")


def plot_multiclient_total_goodput(rows: list[dict], outdir: Path) -> None:
    fig, ax = plt.subplots(figsize=(8, 5.5))
    for locks, group in group_rows(rows, "locks").items():
        xs = [row["clients"] for row in group]
        ys = [row["goodput"] for row in group]
        ax.plot(xs, ys, marker="o", linewidth=2, label=f"locks={locks}")

    ax.set_title("Multi-Client CAS Total Goodput vs Clients")
    ax.set_xlabel("Clients")
    ax.set_ylabel("Total goodput (ops/s)")
    ax.set_xscale("log", base=2)
    ax.grid(True, alpha=0.3)
    ax.legend()
    finish_plot(fig, outdir / "multiclient_total_goodput_vs_clients.png")


def plot_multiclient_per_client(rows: list[dict], outdir: Path) -> None:
    fig, ax = plt.subplots(figsize=(8, 5.5))
    for locks, group in group_rows(rows, "locks").items():
        xs = [row["clients"] for row in group]
        ys = [row["goodput"] / row["clients"] for row in group]
        ax.plot(xs, ys, marker="o", linewidth=2, label=f"locks={locks}")

    ax.set_title("Multi-Client CAS Per-Client Goodput vs Clients")
    ax.set_xlabel("Clients")
    ax.set_ylabel("Per-client goodput (ops/s)")
    ax.set_xscale("log", base=2)
    ax.grid(True, alpha=0.3)
    ax.legend()
    finish_plot(fig, outdir / "multiclient_per_client_goodput_vs_clients.png")


def plot_multiclient_p99(rows: list[dict], outdir: Path) -> None:
    fig, ax = plt.subplots(figsize=(8, 5.5))
    for locks, group in group_rows(rows, "locks").items():
        xs = [row["clients"] for row in group]
        ys = [row["p99_us"] for row in group]
        ax.plot(xs, ys, marker="o", linewidth=2, label=f"locks={locks}")

    ax.set_title("Multi-Client CAS P99 vs Clients")
    ax.set_xlabel("Clients")
    ax.set_ylabel("P99 latency (us)")
    ax.set_xscale("log", base=2)
    ax.grid(True, alpha=0.3)
    ax.legend()
    finish_plot(fig, outdir / "multiclient_p99_vs_clients.png")


def write_summary(single_client_rows: list[dict], multiclient_rows: list[dict], outdir: Path) -> None:
    lines = ["Professor plot summary", ""]
    lines.append("Single-client best points:")
    for row in best_rows_by_lock(single_client_rows):
        lines.append(
            f"  locks={row['locks']}: best_window={row['active_window']}, "
            f"goodput={int(row['goodput']):,} ops/s, p99={row['p99_us']:.2f} us"
        )

    lines.append("")
    lines.append("Multi-client sanity checks (window=16):")
    for row in sorted(multiclient_rows, key=lambda r: (r["locks"], r["clients"])):
        lines.append(
            f"  locks={row['locks']}, clients={row['clients']}: goodput={int(row['goodput']):,} ops/s, "
            f"per_client={int(row['goodput'] / row['clients']):,} ops/s, p99={row['p99_us']:.2f} us"
        )

    (outdir / "summary.txt").write_text("\n".join(lines) + "\n")


def main() -> None:
    OUTDIR.mkdir(parents=True, exist_ok=True)
    rows = aggregate_rows(load_rows())

    single_client_all = [row for row in rows if row["clients"] == 1]
    single_client_core = [row for row in single_client_all if row["locks"] in {1, 5, 16, 100, 1000}]
    single_client_large = [row for row in single_client_all if row["locks"] in {1000, 5000, 10000}]

    multiclient = [
        row
        for row in rows
        if row["active_window"] == 16 and row["locks"] in {1000, 10000} and row["clients"] in {1, 2, 4, 8}
    ]

    plot_single_client_goodput(single_client_core, OUTDIR)
    plot_single_client_goodput_large(single_client_large, OUTDIR)
    plot_single_client_p99(single_client_core, OUTDIR)
    plot_best_goodput_vs_locks(single_client_all, OUTDIR)
    plot_best_window_vs_locks(single_client_all, OUTDIR)
    plot_multiclient_total_goodput(multiclient, OUTDIR)
    plot_multiclient_per_client(multiclient, OUTDIR)
    plot_multiclient_p99(multiclient, OUTDIR)
    write_summary(single_client_all, multiclient, OUTDIR)

    print(f"Generated professor plots in {OUTDIR}")


if __name__ == "__main__":
    main()
