# Motex-fetcher-core

通用抓取框架核心：两阶段（index → crawl）调度、断点续传、二进制下载、BFS 发现。
各项目通过扩展接入自己的站点/数据源，核心保持零项目个性化。

## 能力

- **两阶段管线**：index（发现/建池）→ crawl（抓取/落盘），断点 checkpoint 续跑
- **调度器**：并发控制 / 重试 / 指数退避 / URL 去重 / 429 自适应降并发 / 失败重入队 / 兜底重试轮
- **存储**：JSONL 落盘 + **64MB 分片自动切换**（`.partN`）+ 已爬记录（`.done.urls` 快路径）
- **二进制下载**：`downloadRaw` 源 → 独立 worker 进程（Range 1MB 分块并行 / 断点续传 / 任意认证头），抗慢网与大文件
- **发现通道**：站点处理器注册表（`registerSiteHandler`）+ 站内 BFS 全遍历（参数化）+ 追更框架
- **日志**：tlog 结构化 JSONL（按站分文件、心跳、stall 告警）
- **优雅暂停**：`pause_crawls.flag` 检测 → checkpoint 落盘 → 干净退出

## 用法

```bash
npm install          # 依赖 @deepseek-ai/cordis、cheerio
npm run self-test    # 自检（框架完整性）
```

### 扩展一个站点

```ts
// my-site.ts（你的项目里）
import { registerSiteHandler } from 'motex-fetcher-core'
registerSiteHandler({
  id: 'my-site',
  discover: async (ctx, source) => {
    // 返回待抓 URL 列表（可 pushIndex 入池）
    return ['https://example.com/a', 'https://example.com/b']
  },
})
```

```jsonc
// config.json
{
  "sources": [{ "id": "my-site", "kind": "site", "siteHandler": "my-site", "parseRule": { "encoding": "utf-8", "section": "#content", "minLen": 40 } }],
  "scheduler": { "concurrency": 8, "stateFile": "state/my-site/run.json" },
  "storage": { "outDir": "out" }
}
```

```bash
node --experimental-strip-types src/index.ts --config config.json --phase both
```

### 二进制下载（大文件源）

```jsonc
{ "id": "audio", "kind": "index", "downloadRaw": true,
  "seedUrls": ["https://example.com/files/"], "indexRule": { "linkRegex": "href=\"([^\"]+\\.tar\\.gz)\"" } }
```

## 目录

```
src/
├── index.ts              # 入口：两阶段主流程 + 断点恢复 + 优雅暂停 + 自检
├── config.ts / types.ts / rules.ts / trace.ts
├── services/             # scheduler / indexer / parser / storage（分片）/ proxy
├── plugins/pipeline.ts   # 事件接线
├── discovery/            # registry（站点注册表）/ bfs / update
└── download_worker.ts    # 大文件下载 worker（独立进程）
```


## 管理服务（可选）

```jsonc
{ "manage": { "enabled": true, "port": 8787, "api": true, "web": true } }
```

- `GET /api/status` 任务状态（输出/心跳/暂停态）
- `POST /api/pause` 优雅暂停（写 pause flag → checkpoint 干净退出）
- `POST /api/resume` 恢复（看护自动拉起）
- `GET /` 轻量 Web 管理页（`web: true` 时）
- `POST /api/instances` 图形化新增实例入口（接口预留，未实现）

默认关闭（不配 `manage` 即不启动）。
## 许可

MIT