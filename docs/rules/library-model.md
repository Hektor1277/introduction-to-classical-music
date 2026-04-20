# 项目存储模型

本文档从第三方开发者视角描述当前项目的核心存储结构、字段职责与修改边界，用于约束后续的数据清洗、自动检查与界面实现。

## 1. 顶层实体

### `composer`

- 负责作曲家 `canonical identity`（规范身份）。
- `name` 必须是中文全名。
- `fullName` 保留完整中文全名，兼容历史字段。
- `nameLatin` 为英文或原文全名。
- `displayName`、`displayFullName`、`displayLatinName` 是展示层优先字段。
- `aliases`、`abbreviations` 只作为检索与兼容信息，不得覆盖规范主名。

### `person`

- 负责非作曲家人物与各类团体。
- `roles` 支持多角色，允许包含 `composer`，避免“作曲家”和“人物”双轨建模。
- 常见角色：
  - `composer`
  - `conductor`
  - `soloist`
  - `singer`
  - `instrumentalist`
  - `orchestra`
  - `ensemble`
  - `chorus`
- 具有团体角色的 `person` 仍然存放在 `people` 集合中，而不是另建 `group` 集合。

### `work`

- 负责作品 `canonical identity`（规范身份）。
- `title` 为中文主标题。
- `titleLatin` 为原文或英文标题。
- `catalogue` 为作品号。
- `groupIds` 指向作品分组，用于推断体裁、展示家族与检索分面。

### `recording`

- 负责具体版本、演出、录音或录像。
- `workId` 指向所属作品，是唯一权威作品关联。
- `credits[]` 是唯一权威参与者结构。
- `workTypeHint` 是输入层体裁提示，会驱动展示家族、标题规则和 owner 表单快捷入口。
- `title` 是派生字段，不是原始权威事实。
- `performanceDateText`、`venueText`、`albumTitle`、`label`、`releaseDate` 为展示层元数据。
- `legacyPath` 仅用于回溯原始档案，不参与展示。

## 2. `recording` 的核心约束

### 唯一权威参与者结构

- 所有参与者都必须落在 `credits[]` 中。
- 不新增第二套平行字段保存“多个独奏者”或“多个团体”。
- `conductorPersonId`、`orchestraPersonId` 仅作为 owner 表单快捷入口，保存时必须同步回 `credits[]`。

### `credits[]` 基础结构

每条 `credit` 至少包含：

- `role`
- `displayName`
- `personId` 可选

补充约束：

- `displayName` 负责保底显示，不能完全依赖 `personId` 反查。
- `personId` 为空时，表示当前仍处于历史脏数据、人工补录或待确认状态。
- 允许同一版本出现任意数量的以下角色：
  - 多个 `soloist`
  - 多个 `singer`
  - 多个 `instrumentalist`
  - 多个 `ensemble`
  - 多个 `chorus`
  - 多个团体混合出现，例如 `orchestra + chorus`

### 复数人物 / 团体兼容策略

- 歌剧、芭蕾、清唱剧、合唱交响曲等包含人声或多团体合作的体裁，必须通过多条 `credit` 表达。
- 室内乐临时组合也必须通过多条 `soloist` 或 `instrumentalist` 表达，不得压缩为伪组合名。
- 标题、搜索文本、详情页显示都必须从 `credits[]` 按体裁规则派生，而不是依赖手写字符串拼接。

## 3. 归档字段与派生字段

### 原始事实字段

以下字段应尽量直接来自 `archive`（原始档案）或人工确认结果：

- `composer.name` / `person.name` / `work.title`
- `nameLatin` / `titleLatin`
- `catalogue`
- `credits[]`
- `performanceDateText`
- `venueText`
- `albumTitle`
- `label`
- `releaseDate`
- `legacyPath`

### 派生字段

以下字段可由规则模块统一生成或重建：

- `recording.title`
- 各实体的搜索展示文案
- 首页推荐卡片显示文案
- owner 端关联条目分组标签
- `slug`
- `sortKey`
- 由 `work` 与 `workGroup` 推断得到的 `workTypeHint`

## 4. `slug` 与 `sortKey`

- `slug` 与 `sortKey` 属于项目内部管理字段，不属于外部客观事实。
- owner 工具负责统一生成、覆盖与修复。
- 用户手工输入仅可作为临时参考，不视为权威来源。
- 数据清洗脚本允许在保持实体身份不变的前提下重建这两个字段。

## 5. 一级直接关联

owner 端关联跳转只显示一级直接关联，避免界面退化为图遍历器：

- `composer -> work`
- `work -> composer`
- `work -> recording`
- `person -> recording`
- `recording -> work`
- `recording -> credited people`

版本关联条目允许在 `版本 / 作曲家 / 曲目` 这一层做二次分组展示，但底层关系仍然是一级 `recording -> work -> composer`。

## 6. 代码问题与数据问题的边界

### 属于代码问题

- 同一规则在多个模块重复实现。
- owner 与 site 对同一字段含义解释不一致。
- 展示标题、副标题与搜索文本拼装规则不统一。
- 自动检查候选的去重、风控、阻断逻辑不一致。

### 属于数据问题

- 版本缺少关键 `credit`。
- 乐团、人物或作品仍然使用别名充当规范主名。
- 历史 `title` 与结构化 `credits[]` 不一致。
- 占位实体如 `person-item`、`-`、`unknown` 仍被正式条目引用。
- 原始档案本身缺字段，导致无法安全自动回填。

## 7. 当前新增的清洗工件

- `materials/references/manual-recording-backfills.json`
  - 记录受控手工回填规则。
- `materials/references/manual-recording-backfills.unresolved.json`
  - 记录仍需人工补录的版本问题队列。

这两个文件属于清洗阶段的规则与工件，不属于正式库实体的一部分，但后续清洗必须以它们为边界执行。

## 8. 变更原则

- 规则变更优先修改共享模块，再修改 owner 与 site。
- 个别条目异常优先作为数据清洗问题处理，不在展示层打补丁。
- 新增体裁、新增显示规则或新增清洗逻辑时，先更新本规则文档，再改代码和测试。
