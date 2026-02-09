#!/usr/bin/env python3
import json
import math
import pathlib
import sys

if len(sys.argv) != 5:
    print("usage: check_bench_regression.py <baseline.json> <criterion_dir> <baseline_name> <threshold>")
    sys.exit(2)

baseline_path = pathlib.Path(sys.argv[1])
criterion_dir = pathlib.Path(sys.argv[2])
baseline_name = sys.argv[3]
threshold = float(sys.argv[4])

if not baseline_path.exists():
    print(f"missing baseline file: {baseline_path}")
    sys.exit(2)

baseline = json.loads(baseline_path.read_text())
benchmarks = baseline.get("benchmarks", {})
if not benchmarks:
    print("baseline has no benchmarks")
    sys.exit(2)

errors = []
results = {}

for display_name, meta in benchmarks.items():
    criterion_name = meta.get("criterion")
    budget_factor = float(meta.get("max_regression_factor", 1.0 + threshold))
    estimates = criterion_dir / criterion_name / baseline_name / "estimates.json"
    if not estimates.exists():
        errors.append(f"missing estimates for {display_name}: {estimates}")
        continue

    payload = json.loads(estimates.read_text())
    point = payload.get("mean", {}).get("point_estimate")
    if point is None:
        errors.append(f"missing mean point_estimate for {display_name}")
        continue

    baseline_ns = float(meta.get("baseline_ns"))
    allowed_ns = baseline_ns * budget_factor
    regression = float(point) / baseline_ns if baseline_ns > 0 else math.inf

    results[display_name] = {
        "point_ns": float(point),
        "baseline_ns": baseline_ns,
        "allowed_ns": allowed_ns,
        "ratio": regression,
    }

    if point > allowed_ns:
        errors.append(
            f"{display_name}: point={point:.0f}ns baseline={baseline_ns:.0f}ns allowed={allowed_ns:.0f}ns ratio={regression:.3f}"
        )

print(json.dumps({"results": results, "threshold": threshold}, indent=2, sort_keys=True))

if errors:
    print("\nregressions detected:")
    for err in errors:
        print(f"- {err}")
    sys.exit(1)

sys.exit(0)
