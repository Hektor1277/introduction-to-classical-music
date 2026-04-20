# 历史脏数据清洗规则

本文档定义原始库清洗、审计与受控修复的边界，确保清洗阶段尽量自动化，但不会误伤现有正式功能。

## 1. 清洗目标

- 清除占位实体与占位 `credit`
- 回填版本体裁、结构化参与者与派生标题
- 识别人物、团体、作品、版本中的历史脏字段
- 为后续自动检查提供稳定、一致的基础数据

## 2. 审计层输出格式

`library audit` 统一输出：

- `code`
- `severity`
- `entityType`
- `entityId`
- `message`
- `source`
- `suggestedFix`

## 3. 第一批审计规则

### `placeholder-entity`

覆盖：

- `person-item`
- `-`
- `unknown`
- `未知`
- 空白或未填写占位值

处理原则：

- 能通过 `archive`（原始档案）安全回读并定位正式实体时，优先替换引用。
- 无法安全判断时，只输出审计问题，不自动删除。

### `recording-missing-credit-role`

按展示家族检查关键参与者是否缺失：

- `orchestral`
  - 至少应有 `conductor` 与 `orchestra / ensemble / chorus` 之一
- `concerto`
  - 至少应有独奏者与团体
- `opera`
  - 至少应有 `conductor`、主要表演者与团体
- `solo`
  - 至少应有独奏者或等效主参与者
- `chamber`
  - 至少应有组合或多位主参与者

### `recording-work-type-conflict`

- `recording.workTypeHint` 与所属作品推断体裁不一致时发出审计警告。
- 优先人工核对作品分组与版本体裁。

### `recording-title-credit-mismatch`

- 历史标题与当前结构化 `credits[]` 推导标题不一致时发出审计警告。
- 优先保留结构化 `credits[]`，再重建 `recording.title`。

## 4. 自动清洗与人工复核边界

### 允许自动修复

- `workTypeHint` 可由作品上下文稳定推断
- 占位 `credit` 可通过 `archive` 明确回填到正式实体
- 历史标题可由完整结构化 `credits[]` 稳定重建
- 错误的文件名推导 `credit` 可通过原始 HTML 否定后移除
- 纯地点误写进 `performanceDateText` 的情况，可安全移回 `venueText`

### 必须人工复核

- 多个候选实体都可能匹配同一脏条目
- 版本缺失关键参与者，且 `archive` 也无法稳定回读
- 人物 / 团体别名已经互相污染
- 作品体裁与版本 `credit` 同时冲突
- 原始档案本身缺字段，只能人工补录

## 5. 手工回填与未决队列

### `materials/references/manual-recording-backfills.json`

- 存放受控手工回填规则。
- 允许描述：
  - `removeCredits`
  - `credits`
  - `metadata`
- `metadata` 当前允许覆盖：
  - `performanceDateText`
  - `venueText`
  - `albumTitle`
  - `label`
  - `releaseDate`

### `materials/references/manual-recording-backfills.unresolved.json`

- 存放仍需人工补录的未决版本问题。
- 由审计脚本统一导出，不手工拼接。
- 该文件是后续人工清洗与证据补录的队列，不属于正式库数据。

## 6. 推荐执行顺序

1. 先跑 `library audit`，得到全库问题地图。
2. 先修占位实体与占位 `credit`。
3. 再回填 `workTypeHint` 与 `recording.title`。
4. 再处理人物 / 团体 alias 污染与重复条目。
5. 最后再用自动检查对清洗后的标准库做候选优化。

## 7. 安全策略

- 所有清洗脚本优先支持 `dry-run`。
- 每个阶段完成后必须重新运行：
  - `npm test --runInBand`
  - `npm run runtime:build`
  - `npm run build`
- 每个阶段单独提交并推送，避免把清洗、规则改动和 UI 回归混在一个提交里。
