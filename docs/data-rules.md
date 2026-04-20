# 数据规则与索引规范

本文档定义本项目当前采用的数据结构、显示规则、搜索规则、图片规则与自动检查规则。后续所有人工录入、自动检查、候选审查与索引生成，均应以此文档为准。

## 1. 实体类别

项目当前只允许以下一层实体：

- `Composer`：作曲家。
- `Person`：人物与团体。通过 `roles` 区分为 `conductor`（指挥）、`soloist`（独奏）、`singer`（歌手）、`instrumentalist`（器乐演奏者）、`ensemble`（组合）、`chorus`（合唱）、`orchestra`（乐团）、`other`。
- `WorkGroup`：作品类型树节点。
- `Work`：具体作品。
- `Recording`：推荐版本。

所有页面索引、搜索索引、候选审查与自动检查都只能围绕这五类实体展开，不得再引入平行但语义重叠的隐藏实体类型。

## 2. 命名字段规范

### 2.1 作曲家与人物

对 `Composer` 与 `Person`，当前统一采用以下命名字段：

- `name`
  中文常用名或俗名。允许是短称，如“马勒”“贝多芬”，但必须稳定。
- `fullName`
  中文全名。若 `name` 为短称，则此字段必须填写。
- `nameLatin`
  英文、拉丁转写或母语拉丁化全名。
- `displayName`
  规范短显示名。通常等于 `name`。
- `displayFullName`
  规范中文全称。通常等于 `fullName`。
- `displayLatinName`
  规范英文或原文全称。不得混入年份、国家、角色说明等附加信息。
- `aliases`
  别名、旧译名、俗称、常见误写、旧团名、中文全称候选。
- `abbreviations`
  简称与缩写。尤其适用于乐团、组合、合唱团，例如 `VPO`、`BPO`。

### 2.2 命名使用原则

- 网站端主标题统一优先使用 `displayFullName`，若缺失则回退到 `fullName`。
- `displayName` 与 `name` 作为短显示名、卡片短标签、内部编辑与别名补录使用，不再作为网站主标题。
- 若 `name` 为中文短称，则 `fullName` 与 `displayFullName` 不得为空。
- `displayLatinName` 必须只保留名字，不得包含年份、国家、职业或注释。
- `aliases` 中应允许保留中文全名、旧译名、俗称、旧团名，但不得把角色说明或长句整段塞入别名。

## 3. 网站显示规则

### 3.1 人名与团体名

- 网站主显示名统一使用中文全名。
- 网站副显示名可展示短称或常用称呼。
- 第三层显示使用 `displayLatinName`。
- 目录页、详情页、搜索页、人物页、乐团页、作曲家页必须使用同一套规范显示逻辑。

### 3.2 作品与版本

- `Work` 主标题使用作品中文标题。
- `Recording` 列表标题不再直接使用 `recording.title` 作为展示名，而由演出人员自动拼装：
  - 指挥
  - 乐团
  - 独奏 / 歌手 / 器乐
  - 组合 / 合唱
- 版本页副标题展示作品名、作曲家与时间地点信息。
- 所有重复性 boilerplate 文案不再出现在版本列表中。

## 4. 搜索与索引规则

### 4.1 搜索主字段

搜索索引对每条实体至少包含以下概念字段：

- `primaryText`
  主显示文本。
- `secondaryText`
  副显示文本。
- `matchTokens`
  规范匹配词。
- `aliasTokens`
  别名、简称、缩写。
- `kind`
  实体类型。

### 4.2 搜索归一化

搜索统一进行以下归一化：

- `NFKC` 规范化。
- 忽略大小写。
- 忽略全半角差异。
- 忽略常见标点、连字符、点号与中点差异。
- 支持中英混搜。
- 支持旧译名、简称、缩写与常见变体搜索。

### 4.3 索引入口原则

- 全站目录始终先到实体层，不直接把版本列表当一层入口。
- 作曲家页先到 `WorkGroup`，再进入 `Work`。
- 人物页、乐团页通过 `roles` 派生不同入口，但底层仍指向同一 `Person`。
- 版本应能通过作品、指挥、乐团三类路径稳定进入。

## 5. 图片规则

### 5.1 人物与团体

对 `Composer` 与 `Person` 当前使用以下图片字段：

- `avatarSrc`
- `imageSourceUrl`
- `imageSourceKind`
- `imageAttribution`
- `imageUpdatedAt`

选图原则：

- 人物优先清晰头像、历史照片、权威媒体图。
- 乐团、组合、合唱优先官方或权威媒体释出的团体照。
- `logo`、`icon`、`placeholder`、`baidu logo`、`signature`、`autograph`、`wordmark` 一律视为无效图。

### 5.2 版本

`Recording.images[]` 允许多图，但应区分用途：

- `cover`
  专辑或发行封面。
- `performance`
  现场照片。
- `artist`
  艺术家宣传照。

选图原则：

- 专辑版优先专辑封面。
- 现场版优先该场次官方照片。
- 无可靠现场图时可留空，禁止用站点 logo、纯文字图或无关图片代替。

## 6. 自动检查规则

### 6.1 来源优先级

当前自动检查的字段候选优先级为：

1. `Wikipedia`
2. `Baidu Baike`
3. `LLM`
4. `Baidu Search`
5. `Wikimedia Commons` 主要用于图片与补充说明

`LLM` 现在是主动候选来源，而非只在缺口时被动补位。

### 6.2 状态语义

自动检查条目状态统一解释如下：

- `succeeded`
  生成了可复核候选。
- `completed-nochange`
  本轮未生成新增候选，且按当前规则判断条目已较完整。
- `needs-attention`
  本轮没有可直接采用的新增候选，或候选不足以补齐条目，仍需人工复核或补录。
- `failed`
  本轮检查发生真实错误，例如来源访问失败、解析失败或运行时异常。

### 6.3 结果落地原则

- 自动检查只能生成候选，不能直接覆盖正式数据。
- 图片候选也必须进入候选审查。
- 单条检查与批量检查共享同一规则。
- 若没有新增候选，也必须给出明确结论，而不能一律视作失败。

## 7. 当前硬校验

当前 `validateLibrary()` 至少保证：

- 所有实体 `id` 唯一。
- `WorkGroup` 必须引用存在的 `Composer`。
- `Work` 必须引用存在的 `Composer` 与 `WorkGroup`。
- `Recording` 必须引用存在的 `Work`。
- `Recording.credit.personId` 若存在，则必须引用存在的 `Person`。
- `Recording.images[].src` 不可为空。
- `Recording.links` 至少保留一个外部资源链接。

## 8. 当前 issue 提示规则

当前 `collectLibraryDataIssues()` 会显式提示以下问题：

- `name-normalization`
- `year-conflict`
- `country-missing`
- `abbreviation-missing`
- `summary-missing`

这些 issue 是“待清洗问题”，不是构建硬错误；但应作为维护工具优先处理对象。

## 9. 数据清洗优先级

当前最优先清洗顺序如下：

1. `fullName` 与 `displayFullName` 缺失。
2. `displayLatinName` 混入年份、国家、角色说明。
3. 乐团、组合与合唱团缺少 `abbreviations`。
4. 异常图片、站点 logo、占位图未替换。
5. 重复人物、重复乐团、异常 slug 未合并。
6. 版本 credit 未正确映射到 canonical `Person`。

## 10. 当前系统约束

为了保证网站与维护工具长期可维护，当前强制采用以下约束：

- 网站主标题统一走中文全名。
- 搜索与索引统一基于规范字段，不再直接拼原始脏字段。
- `displayName` 负责短显示与索引，`displayFullName` 负责网站主标题。
- 图片必须本地保存或通过受控资源字段引用，不允许把无效占位图当作正式图。
- 自动检查、图片替换、合并建议都必须经过候选审查后才能写入正式数据。
