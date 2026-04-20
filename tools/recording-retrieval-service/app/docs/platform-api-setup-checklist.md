# 平台 API-First 配置清单

本文用于配置 `YouTube`、`Apple Music`、`Bilibili` 的 `API-first`（API 优先）搜索通道。运行时配置文件路径为：

- `config/platform-search.example.json`
- `config/platform-search.local.json`

建议先复制示例文件：

```powershell
Copy-Item '.\config\platform-search.example.json' '.\config\platform-search.local.json'
```

## 1. YouTube Data API

目标：让 `YouTube` 搜索优先走官方 `Data API`，网页搜索仅作降级。

### 用户操作

1. 登录 Google 账号并进入 [Google Cloud Console](https://console.cloud.google.com/).
2. 新建或选择一个 project（项目）。
3. 在 API Library（API 库）中启用 `YouTube Data API v3`。
4. 打开 `Credentials`（凭据）页面，创建 `API key`。
5. 可选：为 API key 增加 API 限制，仅允许 `YouTube Data API v3`。

### 写入配置

```json
{
  "youtube": {
    "enabled": true,
    "apiKey": "YOUR_YOUTUBE_DATA_API_KEY",
    "regionCode": "US",
    "maxResults": 8
  }
}
```

### 验证

```powershell
& '.\.venv\Scripts\python.exe' '.\scripts\real_data_regression.py' `
  --only annie-no-group `
  --output '.\output\verify_youtube_api.json' `
  --access-report '.\output\verify_youtube_api_access.json'
```

检查访问报告中是否出现 `www.googleapis.com`，并优先于 `www.youtube.com/results`。

## 2. Apple Music API

目标：优先走官方 `Apple Music API`，必要时退回 `iTunes Search API`，最后才回退网页搜索。

### 用户操作

1. 准备 Apple ID。
2. 登录 [Apple Developer](https://developer.apple.com/).
3. 如果页面要求双重验证、短信验证码或设备确认，按页面提示手动完成。
4. 若要使用正式 `Apple Music API`，通常需要 Apple Developer Program 资格与生成 `developer token`。
5. 如果当前不准备加入付费开发者计划，仍可先使用本工具内置的 `iTunes Search API` 降级路径，不会阻塞开发。

### 写入配置

```json
{
  "appleMusic": {
    "enabled": true,
    "developerToken": "YOUR_APPLE_MUSIC_DEVELOPER_TOKEN",
    "storefront": "us",
    "useItunesFallback": true
  }
}
```

### 验证

```powershell
& '.\.venv\Scripts\python.exe' '.\scripts\real_data_regression.py' `
  --only annie-full `
  --output '.\output\verify_apple_api.json' `
  --access-report '.\output\verify_apple_api_access.json'
```

检查访问报告中：

- 若已配置 `developerToken`，应出现 `api.music.apple.com`
- 若未配置但 `useItunesFallback=true`，应出现 `itunes.apple.com`
- `music.apple.com/search` 只应在 API 路径失败时作为降级出现

## 3. Bilibili API

目标：优先走 `api.bilibili.com/x/web-interface/search/type`，网页搜索只保留降级路径。

### 用户操作

1. 登录 Bilibili 账号。
2. 如果要求短信验证码、滑块验证、人机校验或设备确认，按页面提示手动完成。
3. 在浏览器开发者工具中，从已登录请求中复制可用 `Cookie`。
4. 建议同时记录当前浏览器的 `User-Agent`。

### 写入配置

```json
{
  "bilibili": {
    "enabled": true,
    "cookie": "SESSDATA=...; buvid3=...; b_nut=...",
    "userAgent": "Mozilla/5.0 ...",
    "referer": "https://www.bilibili.com"
  }
}
```

### 验证

```powershell
& '.\.venv\Scripts\python.exe' '.\scripts\real_data_regression.py' `
  --only bohm-full `
  --output '.\output\verify_bilibili_api.json' `
  --access-report '.\output\verify_bilibili_api_access.json'
```

检查访问报告中是否优先出现 `api.bilibili.com`，而非 `search.bilibili.com`。

## 4. 环境变量覆盖

如果不想把凭据写入本地 JSON，可改用环境变量：

```powershell
$env:RECORDING_RETRIEVAL_YOUTUBE_API_KEY='...'
$env:RECORDING_RETRIEVAL_APPLE_DEVELOPER_TOKEN='...'
$env:RECORDING_RETRIEVAL_BILIBILI_COOKIE='SESSDATA=...'
```

支持的关键变量：

- `RECORDING_RETRIEVAL_YOUTUBE_API_KEY`
- `RECORDING_RETRIEVAL_YOUTUBE_REGION_CODE`
- `RECORDING_RETRIEVAL_YOUTUBE_MAX_RESULTS`
- `RECORDING_RETRIEVAL_APPLE_DEVELOPER_TOKEN`
- `RECORDING_RETRIEVAL_APPLE_STOREFRONT`
- `RECORDING_RETRIEVAL_APPLE_USE_ITUNES_FALLBACK`
- `RECORDING_RETRIEVAL_BILIBILI_COOKIE`
- `RECORDING_RETRIEVAL_BILIBILI_USER_AGENT`
- `RECORDING_RETRIEVAL_BILIBILI_REFERER`

## 5. 配置完成后的统一验证

```powershell
& '.\.venv\Scripts\python.exe' -m pytest tests -q
& '.\.venv\Scripts\python.exe' '.\scripts\real_data_regression.py' `
  --output '.\output\real_data_round_platform_api_first.json' `
  --access-report '.\output\real_data_round_platform_api_first_access.json'
```

重点查看访问报告中的这些 host（主机）：

- `www.googleapis.com`
- `api.music.apple.com`
- `itunes.apple.com`
- `api.bilibili.com`
- `www.youtube.com`
- `music.apple.com`
- `search.bilibili.com`

理想状态是：前三个平台优先命中 API host，网页 host 仅在 API 不可用时出现。
