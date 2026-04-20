# 自动检查质量规则

本文档描述 automation pipeline（自动检查链路）的质量门槛、去重规则、LLM proposal review（大模型候选复核）边界，以及 owner 应用安全门。

## 1. 总体原则

- 自动检查只生成 `proposal`（候选），不直接写正式库。
- 原始库清洗与自动检查候选生成必须解耦。
- 自动检查只消费清洗后的标准库。
- 任意候选若无法安全直接应用，必须进入人工复核。

## 2. 候选生成前置门槛

进入候选生成前，至少执行以下过滤：

- `entity type filter`
  - 只扫描请求指定的实体类型。
- `issue whitelist`
  - 只处理当前类别允许的字段与问题类型。
- `canonical field completeness check`
  - 现有规范字段已完整且质量高时，不允许低质量来源覆盖。
- `duplicate proposal suppression`
  - 同一实体、同一问题、同一字段修改，只保留一条候选。

## 3. 候选去重

当前使用两层去重：

- `proposal.id` 级去重
  - 去除完全重复的同 ID 候选。
- semantic signature（语义签名）去重
  - 对“同实体、同字段修改、同 merge 候选、同图片来源、同链接来源”进行语义合并。

目标：

- owner 页面不出现重复候选。
- 历史 run 脏数据不会被重复放大。
- 顶层批量应用不会因为重复候选产生假成功或空操作。

## 4. LLM proposal review 边界

LLM 只允许做三类事：

- 拒绝：`verdict = reject`
- 降级 / 标记注意：`verdict = needs-attention`
- 给出标准化建议：`normalizedValue`

LLM 不允许：

- 直接写正式库
- 绕过现有规则门槛强行提升候选可信度
- 覆盖已经存在的高质量 `canonical value`（规范值）

结构化输出至少包含：

- `verdict`
- `status`
- `issues`
- `reasons`
- `confidence`
- `rejectBecause`
- `normalizedValue`

## 5. LLM 安全约束

- `normalizedValue` 只能作为建议，不得自动回写到 `proposal.fields`。
- 低置信度候选必须进入人工复核，当前安全阈值为 `0.75`。
- 只有 LLM 来源、缺少可交叉验证外部依据的候选，不得进入直接应用通道。
- LLM 若认为与现有规范字段冲突，必须转入 `needs-attention` 或 `reject`。

## 6. 风险等级

- `low`
  - 来源稳定、语义清晰、无冲突。
- `medium`
  - 需要人工确认，但仍可保留为候选。
- `high`
  - 存在明显冲突、缺乏交叉验证，或必须依赖人工判断。

`high` 风险候选不得直接应用。

## 7. 直接应用阻断规则

以下候选必须被阻断直接应用：

- `merge proposal`
- `review-only proposal`
- `risk = high`
- 没有可直接写入的字段或图片
- 被 LLM 或本地规则标记为 `needs-attention`
- 与当前库产生实体或字段冲突

这些阻断逻辑必须同时存在于：

- 自动检查后的后端应用逻辑
- owner 顶层批量应用
- owner 单条直接应用入口
- owner 候选卡片显式提示

## 8. owner 行为约束

- “应用当前页 / 应用全部”遇到被阻断候选时必须整体失败并提示原因。
- 不允许静默跳过被阻断候选后继续执行。
- 被阻断候选必须在卡片上有清晰的“阻断应用”说明。
- 若 run 内存在重复、高风险或 review-only 候选，用户必须逐条处理。

## 9. 扩展原则

- 先补失败测试，再改规则。
- 新增来源或新增实体类别前，必须先定义质量门槛和降级策略。
- 仅来自 LLM 的候选默认落入更严格风险等级，不进入自动应用通道。
