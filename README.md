# Motex-fetcher-core

通用抓取框架核心：两阶段（index → crawl）调度、断点续传、二进制下载、BFS 发现。各项目通过 **cordis 插件**扩展接入自己的站点/数据源，核心保持零项目个性化。

## 能力

- **两阶段管线**：index（发现·建池）→ crawl（抓取·落盘），断点 checkpoint 续跑
- **调度器**：并发控制 / 重试 / 指数退避 / URL 去重 / 429 自适应降并发 / 失败重入队 / 兜底重试轮
- **存储**：JSONL 落盘 + **64MB 分片自动切换**（`.partN`）+ 已爬记录（`.done.urls` 快路径）
- **二进制下载**：`downloadRaw` 源 → 独立 worker 进程（Range 1MB 分块并行 / 断点续传 / 任意认证头），抗慢网与大文件
- **cordis 原生装配**：DSH 式 `*.cordis.yml` 声明式启动；站点插件 = cordis 插件（`ctx.provide('site.<id>', handler)`）；用户插件可监听事件/替换服务
- **组合与热更新（HMR）**：`cordis:include` 多文件组合 + `--watch` 文件监听——运行中往 cordis.yml 加一个站点条目，自动应用并开始抓取，全程不重启
- **日志**：tlog 结构化 JSONL（按站分文件、心跳、stall 告警）
- **优雅暂停**：`pause_crawls.flag` 检测 → checkpoint 落盘 → 干净退出

## 快速开始

```bash
npm install
npm run self-test           # 框架自检（main 路径）
npm run self-test-cordis    # cordis 装配链路自检（loader → 站点插件 → fetcher 插件）
```

### 方式 A：cordis.yml 声明式装配（推荐，DSH 同款格式）

```yaml
# app.cordis.yml
- id: my-site
  name: './sites/my-site.ts'      # 站点插件（相对本文件，.ts 直接加载）
  config: { ... }                 # 站点专属配置

- id: fetcher
  name: 'cordis:fetcher'          # 核心抓取器插件（或 'motex-fetcher-core/fetcher'）
  config:
    config: './config.json'       # 抓取器配置（sources/scheduler/storage，见 src/config.ts）
    phase: 'both'                 # 可选：index / crawl / both / update
    concurrency: 4                # 可选：覆盖调度并发
```

```bash
node --experimental-strip-types node_modules/motex-fetcher-core/src/cli.ts --cordis app.cordis.yml
```

### 方式 B：直接跑（简单场景）

```bash
node --experimental-strip-types node_modules/motex-fetcher-core/src/cli.ts --config ./config.json --phase both
```

```bash
# 常驻 + 热更新：运行中新增站点（往 app.cordis.yml 加条目即生效）
node --experimental-strip-types node_modules/motex-fetcher-core/src/cli.ts --cordis app.cordis.yml --watch
```

### 扩展一个站点（cordis 插件 = 一个站）

```ts
// sites/my-site.ts
import { Context } from '@deepseek-ai/cordis'

export default function mySitePlugin(ctx: Context) {
  ctx.provide('site.my-site', {
    id: 'my-site',
    discover: async (ctx, source) => {
      // 抓种子页（走调度器：并发/重试/代理）→ 提取待抓 URL 列表
      const res = await ctx.scheduler.client(source.seedUrls[0], 20000)
      if (!res.ok || !res.body) return []
      const html = new TextDecoder().decode(res.body)
      return extractLinks(html, /href="(\/detail\/\d+\.html)"/g, source.seedUrls[0])
    },
  })
}
```

```jsonc
// config.json
{
  "sources": [{ "id": "my-site", "kind": "site", "siteHandler": "my-site",
                "parseRule": { "encoding": "utf-8", "section": "#content", "minLen": 40 } }],
  "scheduler": { "concurrency": 8, "stateFile": "state/my-site/run.json" },
  "storage": { "outDir": "out" }
}
```

## 目录

```
src/
├── index.ts              # 核心：createApp/assembleApp/runPhase + CLI main
├── fetcher.ts            # fetcher 插件（cordis 插件形式，装配服务 + 跑两阶段）
├── loader.ts             # cordis.yml 装载器（DSH 同款：id/name/config/group/isolate）
├── cli.ts                # CLI 入口（--config / --self-test / --cordis）
├── config.ts / types.ts / rules.ts / trace.ts
├── services/             # scheduler / indexer / parser / storage（分片）/ proxy / manage
├── plugins/pipeline.ts   # 事件接线
├── discovery/            # bfs / update
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

## 文档

- [docs/README.md](docs/README.md)——入门 / 扩展开发 / 架构说明

## 许可

MIT
