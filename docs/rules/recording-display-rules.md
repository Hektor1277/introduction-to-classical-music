# 版本显示规则

本文档约束 `recording`（版本）在 site（网页端）、owner（维护工具）、批量导入模板与自动检查链路中的统一显示方式。

## 1. 字段分层

- `recording.title`
  - 规范版本标题。
  - 属于派生结果，保存时由共享规则统一生成。
- `recording.workTypeHint`
  - 输入层体裁提示。
  - 当前规范值：
    - `orchestral`
    - `concerto`
    - `opera_vocal`
    - `chamber_solo`
    - `unknown`
- `presentation family`（展示家族）
  - 展示层归一化结果，不直接写回库。
  - 当前包括：
    - `orchestral`
    - `concerto`
    - `opera`
    - `solo`
    - `chamber`
    - `unknown`

## 2. 标题与副标题来源

- 标题与副标题必须优先从 `credits[]`、`performanceDateText`、`venueText` 和所属作品体裁派生。
- 历史 `recording.title` 只作回退值，不是权威事实源。
- 多参与者场景必须保留顺序，不允许为了显示方便把多个参与者压扁成一个虚假实体。

## 3. 各展示家族规则

### `orchestral`

- 中文标题：`指挥中文短名 | 乐团 / 合唱 / 团体中文名 | 时间或地点`
- 外文副标题：对应参与者的英文或原文串联。
- 如同时存在 `orchestra` 与 `chorus`，都应进入标题序列。

### `concerto`

- 中文标题：`指挥中文短名 | 独奏者中文短名串联 | 乐团中文名 | 时间或地点`
- 外文副标题：对应英文或原文串联。
- 多独奏者保持 `credits[]` 顺序。

### `opera`

- 中文标题：`指挥 | 重要歌手 / 主演 | 乐团 / 合唱 / 舞团 | 时间或地点`
- 外文副标题：对应英文或原文串联。
- 歌剧、清唱剧、芭蕾等多人物多团体合作版本都适用同一底层规则。

### `solo`

- 中文标题：`独奏者中文全名或短名 | 地点 | 时间`
- 外文副标题：`soloist latin/original | place | time`

### `chamber`

- 中文标题：`组合名或多位参与者串联 | 地点或协作者 | 时间`
- 外文副标题：对应英文或原文串联。
- 没有正式组合条目时，允许多位 `soloist` / `instrumentalist` 直接参与标题生成。

## 4. 搜索与详情页

- 搜索结果标题始终使用 `recording.title`。
- 搜索副文案使用共享规则生成的副标题或次级信息，不直接复用历史原始字段。
- 详情页 `H1` 始终使用 `recording.title`。
- 当副标题与标题不同且有信息增量时，详情页显示副标题。

## 5. 首页推荐卡片

当前首页推荐卡片统一输出 7 行结构：

1. `workPrimary`
   - 中文作品名
   - 加粗并放大一级字号
2. `workSecondary`
   - 原名或英文名
   - 包含作品号
3. `composerPrimary`
   - 作曲家中文全名
4. `composerSecondary`
   - 作曲家英文或原文全名
5. `title`
   - 中文版本标题
   - 加粗
6. `subtitle`
   - 外文版本标题
7. `datePlacePrimary`
   - 时间 / 地点

布局约束：

- 每一行都必须是单行。
- 超出内容不换行，不显示 `...` 字符。
- 使用渐隐遮罩表示被裁切部分。
- 鼠标悬浮通过 `title` 提示展示完整内容。
- 即使某些字段为空，也保留占位行，保证卡片高度一致。

## 6. 去重与回退策略

- 指挥与独奏撞人时，展示层去重，避免同一人同时出现在多条人物线上。
- 当 `performanceDateText` 为空但 `venueText` 有效时，允许标题末尾回退使用地点。
- 当结构化 `credits[]` 完整、历史 `title` 不一致时，优先信任结构化 `credits[]`，并将问题归类为数据清洗。
- 当体裁、参与者或元数据不足以稳定推导标题时，回退到历史 `recording.title`，同时输出审计问题。

## 7. 实现边界

- 共享规则集中在：
  - `packages/shared/src/recording-rules.ts`
  - `packages/shared/src/display.ts`
- owner、site、批量导入和自动检查不得各自实现一套独立标题规则。
- 新增体裁或修改标题规则时，必须先更新本文档，再修改共享实现与测试。
