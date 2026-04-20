# Recording Retrieval Service 协议与接口文档

## 1. 范围
`Recording Retrieval Service` 只负责 `recording`（版本）条目的结构化信息补全。它不是 owner 的子模块，而是一个独立的本地 HTTP 服务。

## 2. 硬边界
- 不得直接写 owner 的 JSON 数据文件。
- 不得直接返回 owner `review state`（审查状态）。
- 不得直接应用任何变更。
- 不负责 `composer / work / person` 自动检查。
- 不负责 batch（批量导入）文本解析。

## 3. 服务标识
- `service`: `recording-retrieval-service`
- `protocolVersion`: `v1`

## 4. HTTP API
### 4.1 Health
`GET /health`

最小响应结构：
```json
{
  "service": "recording-retrieval-service",
  "version": "1.0.0",
  "protocolVersion": "v1",
  "status": "ok"
}
```

### 4.2 Create Job
`POST /v1/jobs`

请求体：
```json
{
  "requestId": "owner-generated-uuid",
  "source": {
    "kind": "owner-entity-check|owner-batch-check",
    "ownerRunId": "run-...",
    "batchSessionId": "batch-...",
    "requestedBy": "owner-tool"
  },
  "items": [
    {
      "itemId": "recording-id",
      "recordingId": "recording-id",
      "workId": "work-id",
      "composerId": "composer-id",
      "workTypeHint": "orchestral|concerto|opera_vocal|chamber_solo|unknown",
      "sourceLine": "原始导入行",
      "seed": {
        "title": "版本标题",
        "composerName": "中文作曲家名",
        "composerNameLatin": "Latin name",
        "workTitle": "中文作品名",
        "workTitleLatin": "Latin title",
        "catalogue": "Op.67",
        "performanceDateText": "1963",
        "venueText": "",
        "albumTitle": "",
        "label": "",
        "releaseDate": "",
        "credits": [],
        "links": [],
        "notes": ""
      },
      "requestedFields": [
        "links",
        "performanceDateText",
        "venueText",
        "albumTitle",
        "label",
        "releaseDate",
        "notes",
        "images"
      ]
    }
  ],
  "options": {
    "maxConcurrency": 4,
    "timeoutMs": 180000,
    "returnPartialResults": true
  }
}
```

成功响应：
```json
{
  "jobId": "provider-job-id",
  "requestId": "owner-generated-uuid",
  "status": "accepted",
  "itemCount": 12,
  "acceptedAt": "2026-03-15T00:00:00.000Z"
}
```

### 4.3 Job Status
`GET /v1/jobs/:jobId`

要求：
- 只返回聚合状态。
- 不返回最终大结果。

结构：
```json
{
  "jobId": "provider-job-id",
  "requestId": "owner-generated-uuid",
  "status": "queued|running|partial|succeeded|failed|timed_out|canceled",
  "progress": {
    "total": 12,
    "completed": 6,
    "succeeded": 5,
    "partial": 1,
    "failed": 0,
    "notFound": 0
  },
  "items": [
    {
      "itemId": "recording-id",
      "status": "queued|running|succeeded|partial|failed|not_found",
      "message": "optional"
    }
  ],
  "logs": [
    {
      "timestamp": "2026-03-15T00:00:00.000Z",
      "level": "info",
      "message": "status update",
      "itemId": "recording-id"
    }
  ],
  "error": "optional"
}
```

### 4.4 Job Results
`GET /v1/jobs/:jobId/results`

终态响应：
```json
{
  "jobId": "provider-job-id",
  "requestId": "owner-generated-uuid",
  "status": "succeeded|partial|failed|canceled|timed_out",
  "completedAt": "2026-03-15T00:00:00.000Z",
  "items": [
    {
      "itemId": "recording-id",
      "status": "succeeded|partial|failed|not_found",
      "confidence": 0.82,
      "warnings": [],
      "result": {
        "performanceDateText": "1963",
        "venueText": "Vienna",
        "albumTitle": "Beethoven: Symphony No. 7",
        "label": "DG",
        "releaseDate": "1964",
        "notes": "简要说明",
        "links": [],
        "images": []
      },
      "evidence": [],
      "linkCandidates": [],
      "imageCandidates": [],
      "logs": []
    }
  ]
}
```

### 4.5 Cancel
`POST /v1/jobs/:jobId/cancel`

返回结构与 `GET /v1/jobs/:jobId` 相同。

## 5. 并行与隔离
- `itemId` 是唯一映射键。
- `requestId + jobId + itemId` 构成完整追踪键。
- 任何缺少 `itemId` 或重复 `itemId` 的结果，owner 会整批拒收。
- 不允许 owner 通过数组顺序推断结果归属。

## 6. 输出语义
- 外部工具输出的是“结构化补全结果”，不是 proposal（候选）。
- owner 会把变化字段翻译成 `AutomationProposal`。
- 如果没有真实变化，owner 可以不生成候选。

## 7. 失败语义
- `unavailable`：owner 无法连接服务或 `/health` 失败。
- `queued`：已接受任务但未开始。
- `running`：正在执行。
- `partial`：已得到部分结果，允许最终返回。
- `succeeded`：全部成功。
- `failed`：全局失败。
- `timed_out`：超时。
- `canceled`：任务被取消。
- `not_found`：单个条目未检索到可信结果。

## 8. owner 翻译规则
owner 只会翻译这些字段：
- `performanceDateText`
- `venueText`
- `albumTitle`
- `label`
- `releaseDate`
- `notes`
- `links`
- `images`

owner 不接受外部工具直接修改这些字段：
- `id`
- `slug`
- `workId`
- `sortKey`
- `isPrimaryRecommendation`

## 9. 建议但不强制的实现行为
- 并行处理 `items`。
- 返回字段级 `evidence`。
- 对链接与图片返回候选列表，而不是只返回最终结论。
- 日志中包含 source（来源）、筛选、拒绝、LLM 介入等关键阶段。
