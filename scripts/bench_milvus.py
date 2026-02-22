#!/usr/bin/env python3
"""
Benchmark Milvus on the exact dataset exported by vectdb-cli.

Usage:
  python3 scripts/bench_milvus.py --dataset /tmp/vectdb_dataset.json --k 10 --nprobe 8
"""

from __future__ import annotations

import argparse
import json
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Sequence, Tuple

import numpy as np

try:
    from pymilvus import (
        Collection,
        CollectionSchema,
        DataType,
        FieldSchema,
        connections,
        utility,
    )
except ImportError as exc:  # pragma: no cover - environment-specific
    raise SystemExit(
        "pymilvus is required. Install with: pip3 install pymilvus"
    ) from exc


@dataclass
class Dataset:
    base_ids: List[int]
    base_vectors: List[List[float]]
    updates: List[Tuple[int, List[float]]]
    queries: List[List[float]]
    dim: int


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run fair Milvus benchmark on vectdb dataset.")
    parser.add_argument("--dataset", required=True, help="Path to dataset JSON from vectdb-cli dump-dataset.")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", default="19530")
    parser.add_argument("--collection", default="")
    parser.add_argument("--k", type=int, default=10)
    parser.add_argument("--nlist", type=int, default=128)
    parser.add_argument("--nprobe", type=int, default=8)
    parser.add_argument("--update-batch", type=int, default=1)
    parser.add_argument("--keep-collection", action="store_true")
    return parser.parse_args()


def load_dataset(path: Path) -> Dataset:
    data = json.loads(path.read_text())
    base = data["base"]
    updates = data["updates"]
    queries = data["queries"]
    if not base:
        raise ValueError("base dataset is empty")
    dim = len(base[0]["values"])
    base_ids = [int(row["id"]) for row in base]
    base_vectors = [list(map(float, row["values"])) for row in base]
    parsed_updates = [(int(row[0]), list(map(float, row[1]))) for row in updates]
    parsed_queries = [list(map(float, row)) for row in queries]
    return Dataset(
        base_ids=base_ids,
        base_vectors=base_vectors,
        updates=parsed_updates,
        queries=parsed_queries,
        dim=dim,
    )


def final_rows(dataset: Dataset) -> Tuple[np.ndarray, np.ndarray]:
    rows: Dict[int, List[float]] = {rid: vec for rid, vec in zip(dataset.base_ids, dataset.base_vectors)}
    for rid, vec in dataset.updates:
        rows[rid] = vec
    ids = np.array(list(rows.keys()), dtype=np.int64)
    vectors = np.array([rows[rid] for rid in ids], dtype=np.float32)
    return ids, vectors


def exact_topk_ids(
    ids: np.ndarray, vectors: np.ndarray, queries: Sequence[Sequence[float]], k: int
) -> List[List[int]]:
    out: List[List[int]] = []
    k_eff = max(1, min(k, vectors.shape[0]))
    for q in queries:
        qv = np.asarray(q, dtype=np.float32)
        dists = np.sum((vectors - qv) ** 2, axis=1)
        idx = np.argpartition(dists, k_eff - 1)[:k_eff]
        pairs = [(int(ids[i]), float(dists[i])) for i in idx]
        pairs.sort(key=lambda x: (x[1], x[0]))
        out.append([pid for pid, _ in pairs[:k_eff]])
    return out


def recall_at_k(got_ids: Sequence[int], expected_ids: Sequence[int], k: int) -> float:
    if k <= 0:
        return 0.0
    got = set(got_ids[:k])
    expected = set(expected_ids[:k])
    return len(got.intersection(expected)) / float(k)


def main() -> None:
    args = parse_args()
    dataset = load_dataset(Path(args.dataset))
    if args.k <= 0:
        raise SystemExit("--k must be > 0")
    if args.update_batch <= 0:
        raise SystemExit("--update-batch must be > 0")

    collection_name = args.collection or f"layerdb_eval_{uuid.uuid4().hex[:10]}"
    connections.connect(alias="default", host=args.host, port=args.port)

    if utility.has_collection(collection_name):
        utility.drop_collection(collection_name)

    fields = [
        FieldSchema(name="id", dtype=DataType.INT64, is_primary=True, auto_id=False),
        FieldSchema(name="vector", dtype=DataType.FLOAT_VECTOR, dim=dataset.dim),
    ]
    schema = CollectionSchema(fields=fields, description="layerdb fairness benchmark")

    build_start = time.perf_counter()
    collection = Collection(name=collection_name, schema=schema, consistency_level="Strong")
    collection.insert([dataset.base_ids, dataset.base_vectors])
    collection.flush()
    collection.create_index(
        field_name="vector",
        index_params={"metric_type": "L2", "index_type": "IVF_FLAT", "params": {"nlist": args.nlist}},
    )
    collection.load()
    build_ms = (time.perf_counter() - build_start) * 1000.0

    update_start = time.perf_counter()
    for base in range(0, len(dataset.updates), args.update_batch):
        batch = dataset.updates[base : base + args.update_batch]
        ids = [row[0] for row in batch]
        vecs = [row[1] for row in batch]
        collection.upsert([ids, vecs])
    collection.flush()
    update_s = max(time.perf_counter() - update_start, 1e-9)
    update_qps = len(dataset.updates) / update_s

    ids, vectors = final_rows(dataset)
    expected = exact_topk_ids(ids, vectors, dataset.queries, args.k)

    search_start = time.perf_counter()
    recall_sum = 0.0
    search_params = {"metric_type": "L2", "params": {"nprobe": args.nprobe}}
    for idx, query in enumerate(dataset.queries):
        result = collection.search(
            data=[query],
            anns_field="vector",
            param=search_params,
            limit=args.k,
            output_fields=[],
        )[0]
        got_ids = [int(hit.id) for hit in result]
        recall_sum += recall_at_k(got_ids, expected[idx], args.k)
    search_s = max(time.perf_counter() - search_start, 1e-9)
    search_qps = len(dataset.queries) / search_s
    avg_recall = recall_sum / max(1, len(dataset.queries))

    print(
        json.dumps(
            {
                "engine": "milvus-ivf-flat",
                "collection": collection_name,
                "dim": dataset.dim,
                "base": len(dataset.base_ids),
                "updates": len(dataset.updates),
                "queries": len(dataset.queries),
                "k": args.k,
                "nlist": args.nlist,
                "nprobe": args.nprobe,
                "update_batch": args.update_batch,
                "build_ms": build_ms,
                "update_qps": update_qps,
                "search_qps": search_qps,
                "recall_at_k": avg_recall,
            },
            indent=2,
        )
    )

    if not args.keep_collection:
        utility.drop_collection(collection_name)
    connections.disconnect(alias="default")


if __name__ == "__main__":
    main()
