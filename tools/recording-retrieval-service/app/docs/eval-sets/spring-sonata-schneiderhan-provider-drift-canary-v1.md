# Spring Sonata Schneiderhan Provider Drift Canary v1

## Scope

- item set: `docs/eval-sets/spring-sonata-schneiderhan-seemann-provider-drift-canary-item-ids-v1.txt`
- included samples:
  - `recording-第5号小提琴奏鸣曲-春天-沃尔夫冈施奈德汉-and-卡尔希曼-full`
  - `recording-第5号小提琴奏鸣曲-春天-沃尔夫冈施奈德汉-and-卡尔希曼-partial`

## Run Command

```powershell
.\.venv\Scripts\python .\scripts\capture_parent_work_snapshot.py `
  --title-latin "Violin Sonata No.5, Op.24" `
  --item-id-file ".\docs\eval-sets\spring-sonata-schneiderhan-seemann-provider-drift-canary-item-ids-v1.txt" `
  --output ".\output\parent_work_eval_spring_sonata_schneiderhan_provider_drift_canary_snapshot_v1.json"
```

## 2026-04-05 Repeat Sampling

- runs: `4`
- summary file:
  - `output/parent_work_eval_spring_sonata_schneiderhan_provider_drift_canary_repeat_summary_v1.json`
- diagnostics file:
  - `output/parent_work_eval_spring_sonata_schneiderhan_provider_drift_canary_repeat_diagnostics_v1.json`

Observed result:

- all 4 runs = `finalHit=0/2`
- strict miss reason stays `recall_miss=2`
- no run recovered target `BV1qW4y1f7Ng` / `BV1NSH9zNE2S` / `youtube:3otH1b0icCg` into final links

Conclusion:

- current behavior is stable provider-side retrieval miss for this pair, not intermittent one-off fluctuation.
