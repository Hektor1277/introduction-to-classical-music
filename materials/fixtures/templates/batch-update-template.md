# 批量导入模板

本模板适用于 owner 工具当前版本的 `batch import（批量导入）`。

## 使用前提
- 先在 owner 中手动建立作曲家条目
- 先在 owner 中手动建立作品条目
- 在批量导入面板顶部先选择：
  - 作曲家
  - 作品
  - 版本模板 `workTypeHint`

## 通用约束
- 每行只允许一个版本
- 字段必须使用 `|` 分隔
- 缺失值必须写 `-`
- 链接列表使用英文逗号 `,` 分隔

## 模板 1: orchestral / 管弦乐
```text
指挥 | 乐团 | 年份 | 链接列表
Kleiber | Wiener Philharmoniker | 1975 | -
Karajan | Berliner Philharmoniker | 1963 | https://example.com/a, https://example.com/b
```

## 模板 2: concerto / 协奏曲
```text
独奏者 | 指挥 | 乐团 | 年份 | 链接列表
Pollini | Kleiber | Wiener Philharmoniker | 1975 | -
```

## 模板 3: opera_vocal / 歌剧与声乐
```text
指挥 | 主演/卡司 | 乐团/合唱 | 年份 | 链接列表
Karajan | Callas / Gobbi | La Scala Orchestra | 1955 | -
```

## 模板 4: chamber_solo / 室内乐与独奏
```text
主奏/组合 | 协作者 | 年份 | 链接列表
Richter | - | 1960 | -
Busch Quartet | - | 1936 | https://example.com
```

## 非法示例
```text
Kleiber 1975 VPO
```
原因：没有 `|` 分隔。

```text
Kleiber | Wiener Philharmoniker | 1975
```
原因：缺少最后一个字段，缺失值也必须显式写成 `-`。
