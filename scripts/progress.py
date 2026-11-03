#!/usr/bin/env python3
"""Live progress of the experiment chain: python3 scripts/progress.py [--interval 5] [--once]"""
import argparse
import os
import time
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
RESULTS = REPO / "results"
LOGS = RESULTS / "logs"
PLAN = {"debit": 6, "latency": 12, "concurrency": 6, "slow": 2, "crash": 2, "broker": 2, "faults": 2,
        "replay": 2, "ordering": 4}


def done_runs(exp):
    return len(list((RESULTS / exp).glob("*.json"))) if (RESULTS / exp).is_dir() else 0


def status_lines():
    path = LOGS / "status.txt"
    return path.read_text().splitlines() if path.exists() else []


def active_log():
    logs = [p for p in LOGS.glob("*.log")]
    return max(logs, key=lambda p: p.stat().st_mtime) if logs else None


def render():
    lines = [f"Progression  {time.strftime('%H:%M:%S')}", "-" * 60]
    total_done = total_plan = 0
    for exp, expected in PLAN.items():
        n = done_runs(exp)
        total_done += min(n, expected)
        total_plan += expected
        bar = "#" * min(n, expected) + "." * max(expected - n, 0)
        lines.append(f"{exp:<12} [{bar}] {n}/{expected}")
    lines += ["-" * 60, f"Total        {total_done}/{total_plan} runs", ""]
    lines.append("Terminés (code de sortie) : " + (", ".join(status_lines()) or "aucun"))
    log = active_log()
    if log:
        tail = [l for l in log.read_text(errors="replace").splitlines() if l.strip()][-4:]
        lines += ["", f"Dernier log : {log.name}"] + [f"  {l[:110]}" for l in tail]
    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--interval", type=float, default=5.0)
    parser.add_argument("--once", action="store_true")
    a = parser.parse_args()
    while True:
        if not a.once:
            os.system("clear")
        print(render(), flush=True)
        if a.once:
            return
        time.sleep(a.interval)


if __name__ == "__main__":
    main()
