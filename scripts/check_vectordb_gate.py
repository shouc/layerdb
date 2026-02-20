#!/usr/bin/env python3
import json
import sys
from pathlib import Path


def load_json(path: Path):
    return json.loads(path.read_text(encoding="utf-8"))


def main() -> int:
    if len(sys.argv) != 5:
        print(
            "usage: check_vectordb_gate.py <config.json> <spfresh.json> <lancedb.json> <summary.json>",
            file=sys.stderr,
        )
        return 2

    config_path = Path(sys.argv[1])
    spfresh_path = Path(sys.argv[2])
    lancedb_path = Path(sys.argv[3])
    summary_path = Path(sys.argv[4])

    config = load_json(config_path)
    spfresh = load_json(spfresh_path)
    lancedb = load_json(lancedb_path)

    thresholds = config["thresholds"]
    failures = []

    sp_recall = float(spfresh["recall_at_k"])
    if sp_recall < float(thresholds["min_spfresh_recall_at_k"]):
        failures.append(
            f"spfresh recall_at_k {sp_recall:.4f} < min {thresholds['min_spfresh_recall_at_k']}"
        )

    sp_search = float(spfresh["search_qps"])
    lc_search = max(float(lancedb["search_qps"]), 1e-9)
    search_ratio = sp_search / lc_search
    if search_ratio < float(thresholds["min_spfresh_search_qps_vs_lancedb_ratio"]):
        failures.append(
            "spfresh/lancedb search_qps ratio "
            f"{search_ratio:.4f} < min {thresholds['min_spfresh_search_qps_vs_lancedb_ratio']}"
        )

    sp_update = float(spfresh["update_qps"])
    lc_update = max(float(lancedb["update_qps"]), 1e-9)
    update_ratio = sp_update / lc_update
    if update_ratio < float(thresholds["min_spfresh_update_qps_vs_lancedb_ratio"]):
        failures.append(
            "spfresh/lancedb update_qps ratio "
            f"{update_ratio:.4f} < min {thresholds['min_spfresh_update_qps_vs_lancedb_ratio']}"
        )

    summary = {
        "spfresh": spfresh,
        "lancedb": lancedb,
        "ratios": {
            "search_qps": search_ratio,
            "update_qps": update_ratio,
        },
        "thresholds": thresholds,
        "ok": len(failures) == 0,
        "failures": failures,
    }
    summary_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")

    if failures:
        print("vectordb benchmark gate failed:")
        for failure in failures:
            print(f"- {failure}")
        print(f"summary: {summary_path}")
        return 1

    print("vectordb benchmark gate passed")
    print(f"summary: {summary_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

