# Frozen Eval Sets

本目录保存 `parent_work_eval`（整作品评估）使用的固定 `itemId`（样本编号）清单，供 `live capture`（实时抓取）和 `frozen replay`（冻结回放）复用。

## 当前集合

- `schumann-op54-drift-smoke-item-ids.txt`
  - `concerto`（协奏曲）高漂移 6 样本集。
  - 用途：区分舒曼钢协 live 波动和代码回归。

- `spring-sonata-chamber-smoke-item-ids.txt`
  - `Spring Sonata`（《春天》小提琴奏鸣曲）8 样本集。
  - 覆盖：`strict hit`、`same_platform_alt_upload`、`recall_miss`。

- `appassionata-chamber-smoke-item-ids.txt`
  - `Appassionata`（《热情》钢琴奏鸣曲）8 样本集。
  - 覆盖：`strict hit`、`final_selection_miss`、`same_platform_alt_upload`、`recall_miss`。

- `appassionata-provider-drift-canary-item-ids-v1.txt`
  - `Appassionata` provider drift（供应方漂移）4 样本哨兵集。
  - 固定 `Gieseking / Sonoda / Petri / Levy` 四条 `full`（完整）样本。
  - 用途：把当前最关键的 `Appassionata` live nondeterminism（实时非确定性）单独冻结，后续优先用它区分代码回归和平台漂移。

- `spring-sonata-recall-probe-item-ids-v2.txt`
  - `Spring Sonata` v2 基线里的 5 条重点 `recall_miss`（召回未命中）样本。
  - 用途：只观察前段召回，不混入 alt upload（替代上传）样本。

- `appassionata-final-selection-probe-item-ids-v2.txt`
  - `Appassionata` v2 基线里剩余的 `final_selection_miss`（最终选择漏选）样本。
  - 当前只有 `園田高弘2023-full` 一条。

- `spring-sonata-provider-drift-canary-item-ids-v1.txt`
  - `Spring Sonata` provider drift（供应方漂移）小型哨兵集。
  - 固定在 `parent_work_eval_spring_sonata_recall_probe_v3.json` 这次曾恢复的样本上。
  - 覆盖：
    - `recording-第5号小提琴奏鸣曲-春天-亚历山大莫吉列夫斯基-and-列奥尼德克鲁策-partial`
      - `strict hit`
    - `recording-第5号小提琴奏鸣曲-春天-亚历山大莫吉列夫斯基-and-列奥尼德克鲁策-full`
      - `strict candidate-only`
  - 用途：验证 provider parsing（供应方解析）修正是否仍能保住这两条样本，不把 live 漂移误判成 chamber 规则退化。

## 运行方式

在仓库根目录 `app` 下执行，统一使用项目虚拟环境。

舒曼钢协 smoke：

```powershell
.\.venv\Scripts\python .\scripts\capture_parent_work_snapshot.py `
  --title-latin 'Piano Concerto, Op.54' `
  --item-id-file '.\docs\eval-sets\schumann-op54-drift-smoke-item-ids.txt' `
  --output '.\output\parent_work_eval_schumann_op54_drift_smoke_snapshot_v1.json'

.\.venv\Scripts\python .\scripts\run_frozen_parent_work_eval.py `
  --snapshot '.\output\parent_work_eval_schumann_op54_drift_smoke_snapshot_v1.json' `
  --item-id-file '.\docs\eval-sets\schumann-op54-drift-smoke-item-ids.txt' `
  --output '.\output\parent_work_eval_schumann_op54_drift_smoke_frozen_results_v1.json'
```

`Spring Sonata` chamber smoke：

```powershell
.\.venv\Scripts\python .\scripts\capture_parent_work_snapshot.py `
  --title-latin 'Violin Sonata No.5, Op.24' `
  --item-id-file '.\docs\eval-sets\spring-sonata-chamber-smoke-item-ids.txt' `
  --output '.\output\parent_work_eval_spring_sonata_chamber_smoke_snapshot_v1.json'
```

`Appassionata` chamber smoke：

```powershell
.\.venv\Scripts\python .\scripts\capture_parent_work_snapshot.py `
  --title-latin 'Piano Sonata No.23, Op.57' `
  --item-id-file '.\docs\eval-sets\appassionata-chamber-smoke-item-ids.txt' `
  --output '.\output\parent_work_eval_appassionata_chamber_smoke_snapshot_v1.json'
```

`Appassionata` provider drift canary：

```powershell
.\.venv\Scripts\python .\scripts\capture_parent_work_snapshot.py `
  --title-latin 'Piano Sonata No.23, Op.57' `
  --item-id-file '.\docs\eval-sets\appassionata-provider-drift-canary-item-ids-v1.txt' `
  --output '.\output\parent_work_eval_appassionata_provider_drift_canary_snapshot_v1.json'

.\.venv\Scripts\python .\scripts\run_frozen_parent_work_eval.py `
  --snapshot '.\output\parent_work_eval_appassionata_provider_drift_canary_snapshot_v1.json' `
  --item-id-file '.\docs\eval-sets\appassionata-provider-drift-canary-item-ids-v1.txt' `
  --output '.\output\parent_work_eval_appassionata_provider_drift_canary_frozen_results_v1.json'
```

更小的 probe（定向探针）：

```powershell
.\.venv\Scripts\python .\scripts\capture_parent_work_snapshot.py `
  --title-latin 'Violin Sonata No.5, Op.24' `
  --item-id-file '.\docs\eval-sets\spring-sonata-recall-probe-item-ids-v2.txt' `
  --output '.\output\parent_work_eval_spring_sonata_recall_probe_v2.json'

.\.venv\Scripts\python .\scripts\capture_parent_work_snapshot.py `
  --title-latin 'Piano Sonata No.23, Op.57' `
  --item-id-file '.\docs\eval-sets\appassionata-final-selection-probe-item-ids-v2.txt' `
  --output '.\output\parent_work_eval_appassionata_final_selection_probe_v2.json'
```

`Spring Sonata` provider drift canary：

```powershell
.\.venv\Scripts\python .\scripts\run_frozen_parent_work_eval.py `
  --snapshot '.\output\parent_work_eval_spring_sonata_recall_probe_v3.json' `
  --item-id-file '.\docs\eval-sets\spring-sonata-provider-drift-canary-item-ids-v1.txt' `
  --output '.\output\parent_work_eval_spring_sonata_provider_drift_canary_frozen_results_v1.json'
```

## 解读规则

- `snapshot summary`（快照摘要）和 `frozen summary`（回放摘要）一致：
  - 说明这次基线已经被稳定冻结，可用于后续代码回归判断。

- `live` 变差但 `frozen replay` 不变：
  - 优先判为 `provider drift`（外部平台漂移）。

- `frozen replay` 也变差：
  - 才进入对应体裁分支排查代码，不跨体裁联动改规则。

## 更新约束

- 不直接覆盖旧快照，生成新版本文件并保留历史。
- 只给当前体裁的样本集增删样本，不混入其他体裁。
- 新样本进入清单前，先说明它代表的是 `strict hit`、`alt upload`、`selection miss` 还是 `recall miss`。
