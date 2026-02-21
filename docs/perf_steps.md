# Performance Step Benchmarks

Dataset and params are fixed across steps:
- `dim=64`
- `base=10000`
- `updates=2000`
- `queries=200`
- SPFresh: `shards=8`, `nprobe=8`, `diskmeta=true`, `update_batch=1024`
- LanceDB: `nlist=96`, `nprobe=8`, `update_batch=1024`

## Baseline (`bcd203b`)

SPFresh:
- `update_qps=200994.08`
- `search_qps=2654.65`
- `recall_at_k=0.6030`

LanceDB:
- `update_qps=131486.32`
- `search_qps=916.59`
- `recall_at_k=0.4970`

SPFresh/LanceDB ratio:
- `update_qps_ratio=1.5286`
- `search_qps_ratio=2.8962`

## Step 1 (`simd-dispatch-cache`)

Change:
- Cache runtime SIMD dispatch once for `dot`/`squared_l2` (x86/AVX2+FMA, x86/AVX2, ARM/NEON) to remove repeated feature probing in the hot path.

SPFresh:
- `update_qps=216926.61` (`+7.93%` vs baseline)
- `search_qps=2655.23` (`+0.02%` vs baseline)
- `recall_at_k=0.6030` (`no change`)

LanceDB:
- `update_qps=123028.03`
- `search_qps=831.82`
- `recall_at_k=0.4830`

SPFresh/LanceDB ratio:
- `update_qps_ratio=1.7632`
- `search_qps_ratio=3.1921`
