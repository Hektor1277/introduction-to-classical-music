# Bilibili Storage State 配置说明

当 `Bilibili` 搜索页或视频页对匿名访问不稳定时，可以使用 `storage state`（登录态持久化）来复用浏览器上下文。

## 作用

- 复用已登录站点上下文，减少每次新建匿名浏览器环境的波动
- 提高 `search.bilibili.com` 和 `www.bilibili.com` 的稳定性
- 让浏览器回退链路与 `WBI search`（WBI 签名搜索）共享更接近真实用户的会话状态

## 采集步骤

在项目根目录执行：

```powershell
& '.\.venv\Scripts\python.exe' '.\scripts\capture_bilibili_storage_state.py' `
  --output '.\config\bilibili-storage-state.json'
```

脚本会：

1. 打开有头浏览器
2. 进入 `https://www.bilibili.com`
3. 等待你手动登录
4. 在你回车确认后保存 `storage state`

## 写入配置

将 `config/platform-search.example.json` 复制为本地配置后，在 `bilibili` 段加入：

```json
{
  "bilibili": {
    "enabled": true,
    "userAgent": "Mozilla/5.0 ...",
    "referer": "https://www.bilibili.com",
    "storageStatePath": "config/bilibili-storage-state.json"
  }
}
```

如果你已经有稳定可用的 `Cookie`，可以与 `storageStatePath` 同时保留；工具会优先复用浏览器登录态，同时补齐请求头。

## Cookie 同步

如果已经保存了 `storage state`，可以直接从该文件中提取 `Cookie header`，不需要手动复制：

```powershell
& '.\.venv\Scripts\python.exe' '.\scripts\sync_bilibili_cookie_from_storage_state.py'
```

这会将 `config/bilibili-storage-state.json` 中的 Bilibili `Cookie` 自动同步到 `config/platform-search.local.json` 的 `bilibili.cookie`。

## 验证

```powershell
& '.\.venv\Scripts\python.exe' '.\scripts\real_data_regression.py' `
  --only bernstein-fantastique-conductor-only gieseking-full `
  --output '.\output\verify_bilibili_storage_state.json' `
  --access-report '.\output\verify_bilibili_storage_state_access.json'
```

重点观察：

- `api.bilibili.com` 是否保持 `healthy`
- `search.bilibili.com` 的 `failureRate` 是否下降
- `www.bilibili.com` 的页面抓取是否更稳定
