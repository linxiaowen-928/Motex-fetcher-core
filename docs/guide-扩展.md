# Motex-fetcher-core 扩展开发指南

> 学会写自己的站点插件、发现逻辑、下载源，并开启管理服务。
> 核心机制是 **cordis**（DSH 同款插件框架）：站点 = 插件，抓取器 = 插件，装配 = cordis.yml。

## 一、扩展点总览

| 扩展点 | 机制 | 适用 |
|---|---|---|
| **站点插件** | cordis 插件：`ctx.provide('site.<id>', handler)` | 自定义"发现 URL"逻辑（分页/分类/去重） |
| **装配声明** | `*.cordis.yml`（DSH 格式） | 声明插件列表 + 顺序 + 配置 |
| **抓取器** | `cordis:fetcher` 插件（核心自带） | 把核心服务装配进上下文树并跑两阶段 |
| 解析规则 | config 的 `parseRule` | 详情页正文提取（无需写代码） |
| 索引源 | `kind: 'index'` + `indexRule.linkRegex` | 目录页正则提取链接 |
| 发现通道 | `discovery/bfs.ts`（站内 BFS） | 全站遍历（不依赖列表页结构） |
| 追更 | `discovery/update.ts`（ongoing 标记） | 连载内容增量更新 |
| 下载源 | `downloadRaw: true` | 二进制大文件（音频/压缩包） |
| 管理服务 | config 的 `manage` | 浏览器/API 看状态、暂停/恢复 |
| **自由扩展** | 事件监听 / 服务替换 / 插件注册 | 统计、入库、告警、魔改任意环节 |

## 二、cordis.yml：声明式装配（DSH 同款）

启动不再靠散落的代码注册，而是**一份清单**——与 DSH 的 `*.cordis.yml` 相同格式：

```yaml
# app.cordis.yml（顶层 = YAML 数组，按顺序应用）
- id: 示例站
  name: './sites/示例站.ts'          # 插件：相对路径（相对本文件）/ 包名 / cordis: 内建
  config: { }                      # 插件配置

- id: fetcher
  name: 'cordis:fetcher'           # 内建抓取器（等价 'motex-fetcher-core/fetcher'）
  config:
    config: './config.json'        # 抓取器配置（sources/scheduler/storage）
    phase: 'both'                  # 可选：index / crawl / both / update
    concurrency: 4                 # 可选：覆盖调度并发
    limit: 0                       # 可选：本次最多抓取 URL 数（0 = 不限）
```

```bash
node --experimental-strip-types node_modules/motex-fetcher-core/src/cli.ts --cordis app.cordis.yml
```

支持字段（与 DSH 对齐）：

| 字段 | 说明 |
|---|---|
| `id` | 条目 id（日志/报错定位） |
| `name` | 插件：相对路径（保留 `.ts` 由 strip-types 加载）/ 包名 / `cordis:group` / `cordis:fetcher` |
| `config` | 插件配置；group 条目 = 子条目数组 |
| `disabled` | `true` 或 `!!js 表达式`（如 `!!js process.platform === 'win32'`） |
| `group` | 分组标记（`name: 'cordis:group'` + `config: [子项]`） |
| `isolate` | 服务隔离（默认无隔离 = root realm 全树共享，站点插件与 fetcher 之间 DI 互通） |

**顺序要求**：站点插件条目必须排在 fetcher 之前（先 `provide('site.<id>')`，fetcher 装配后 indexer 才能经 DI 分派到它）。

### 用户项目里怎么引用核心

核心未发布 npm 时，在你的项目里建 junction 后裸包名可用：

```powershell
# 你的项目目录下（node_modules 里）
New-Item -ItemType Junction -Path node_modules\motex-fetcher-core -Target D:\path\to\Motex-fetcher-core
```

这样 `name: 'motex-fetcher-core/fetcher'`（或 `import { ... } from 'motex-fetcher-core'`）都能解析；`cordis:fetcher` 内建写法则完全不需要安装。

## 三、站点插件（最常用扩展）

### 生命周期

```
cordis.yml 里声明站点插件条目（先于 fetcher）
        ↓
插件 apply：ctx.provide('site.my-site', handler)
        ↓
fetcher 装配核心服务到同一上下文树
        ↓
index 阶段：indexer 发现 → ctx.get('site.my-site') → 调 handler.discover(ctx, source)
        ↓
你返回 URL 列表 → crawl 阶段（并发/重试/断点自动）
        ↓
每个 URL 抓下来 → 按 parseRule 解析 → 落盘
```

### 最小示例

```ts
// sites/my-site.ts
import { Context } from '@deepseek-ai/cordis'
import type { SiteHandler } from 'motex-fetcher-core'

export default function mySitePlugin(ctx: Context) {
  ctx.provide('site.my-site', {
    id: 'my-site',
    discover: async (ctx, source) => {
      // 1. 抓种子页（scheduler.client 走并发/重试/代理）
      const res = await ctx.scheduler.client(source.seedUrls[0], 20000)
      if (!res.ok || !res.body) return []
      const html = new TextDecoder().decode(res.body)
      // 2. 提取链接（extractLinks 自动拼绝对 URL）
      return extractLinks(html, /href="(\/detail\/\d+\.html)"/g, source.seedUrls[0])
    },
  } satisfies SiteHandler)
}
```

### discover 签名

```ts
interface SiteHandler {
  id: string
  discover(ctx: Context, source: SourceConfig): Promise<string[]>
}
```

- **ctx.scheduler.client(url, timeoutMs)**：抓取一个页面（带调度器的重试/限速/代理）
- **ctx.indexer.pushIndex(record)**：往索引池写一条（两阶段分离时用）
- **source**：当前源的配置（seedUrls / handlerConfig / filterKeywords 等）
- **返回**：待抓取的 URL 列表（crawl 阶段处理）

### handlerConfig（站点自定义参数）

```jsonc
{ "siteHandler": "my-site",
  "handlerConfig": { "categories": ["a", "b"], "maxPages": 20 } }
```
在 handler 里读：`(source.handlerConfig ?? {}) as MyConfig`

### filterKeywords（标题过滤）

```jsonc
{ "filterKeywords": ["广告", "加群"] }
```
discover 返回的 URL 如果标题命中关键词，会被过滤——**过滤逻辑在 handler 里自己实现**（用 `source.filterKeywords` 判断），核心不做（每个站过滤规则不同）。

## 四、解析规则 parseRule 全字段

```jsonc
"parseRule": {
  "encoding": "utf-8",            // 页面编码（gbk 站）
  "section": "#content",          // 容器选择器（cheerio）
  "contentSelector": ".txt",      // 正文元素选择器
  "title": "h1",                  // 标题选择器（可选）
  "adLineRegex": ["www\\.", "广告"],  // 广告行过滤
  "minLen": 80,                   // 正文最小长度
  "chapterSignal": "chapter-list",// 详情页信号（列表页不落盘，只入队章节）
  "chapterLinkPrefix": "/novel/", // 章节链接前缀
  "followPagination": true        // 分页跟随（正文分多页时拼接）
}
```

## 五、二进制下载源（音频/压缩包）

```jsonc
{ "id": "audio", "kind": "index", "downloadRaw": true,
  "seedUrls": ["https://example.com/files/"],
  "indexRule": { "linkRegex": "href=\"([^\"]+\\.tar\\.gz)\"" } }
```

- 抓到的文件不进内存，**流式落盘**（独立 worker 进程，Range 分块抗慢网）
- 断点续传：完成一个记一个（`data_audio/<source>.meta.jsonl`）
- 认证：worker 支持 `authHeaders`（如 `{ "Authorization": "Bearer xxx" }`）或 `hfToken`（兼容）

## 六、管理服务（可选开启）

```jsonc
{ "manage": { "enabled": true, "port": 8787, "api": true, "web": true } }
```

- `GET /api/status`：任务状态（输出/心跳/暂停态）
- `POST /api/pause`：优雅暂停（写 flag → 任务 checkpoint 干净退出）
- `POST /api/resume`：恢复（看护自动拉起）
- `GET /`：浏览器管理页（自动刷新）

## 七、优雅暂停机制（了解即可）

```
crawl_ctl 或管理服务 → 写 state/pause_crawls.flag
        ↓
运行中的任务每 5s 检测 flag → checkpoint 落盘 → 干净退出（exit 0）
        ↓
删除 flag → 看护器/手动重新拉起 → 断点续跑
```

## 八、最佳实践

1. **发现幂等**：discover 返回的 URL 重复没关系（scheduler 去重 + done.urls 兜底）
2. **礼貌限速**：慢站用 `concurrency: 2` + `requeueDelayMs: 8000` 起步
3. **验证一次**：fetcher 配置里先 `limit: 5` 小批跑通，再看 out/ 产物确认解析正确
4. **大文件源**：用 `downloadRaw` + 独立 worker（不要走正文管线）
5. **断点是资产**：`state/`、`pool/`、`out/` 都是可恢复资产，别随手删

**下一步**：读《架构说明》（调度器内部/断点格式/事件日志）。

## 九、自由扩展（cordis 能力——不再黑盒）

核心的装配单位就是 cordis 本身。除了 cordis.yml 里的站点插件，还有以下扩展面：

### 1. 用户插件（监听事件）

```ts
// my-stats.ts —— 放进 cordis.yml 即可
import { Context } from '@deepseek-ai/cordis'

export default function myStats(ctx: Context) {
  ctx.on('fetch/parsed', (res, item) => { /* 每条落盘前：统计/入库/转发 */ })
  ctx.on('fetch/failed', (res) => { /* 失败告警 */ })
  ctx.on('fetch/response', (res) => { /* 全量响应流 */ })
}
```

### 2. 替换核心服务（继承默认类魔改）

```ts
import { createApp, SchedulerService, loadConfig } from 'motex-fetcher-core'

class MyScheduler extends SchedulerService {
  // 魔改：例如自定义 429 策略 / 额外统计
  protected override noticeLimited(now: number) { /* ... */ }
}

const app = createApp(loadConfig('./config.json'), {
  services: { scheduler: MyScheduler },   // 换掉调度器，其余默认
})
```

### 3. 只拿 app 自己玩（不跑两阶段）

```ts
import { createApp, loadConfig } from 'motex-fetcher-core'

const app = createApp(loadConfig('./config.json'))
await app.scheduler.push(['https://example.com/a'], 'demo', 0)  // 直接用调度器
await app.scheduler.waitIdle()                                  // 等完成
// 落盘逻辑在 pipeline 插件里（fetch/response → parse → storage）
```

### 4. 程序化跑两阶段（等价 cordis:fetcher 插件内部）

```ts
import { createApp, runPhase, loadConfig } from 'motex-fetcher-core'

const app = createApp(loadConfig('./config.json'))
const { paused } = await runPhase(app, cfg, { phase: 'both', concurrency: 4 })
```

### 5. 把核心装进你自己的 cordis 应用

```ts
import { assembleApp, runPhase } from 'motex-fetcher-core'
// app 是你的 cordis Context（任何来源：new Context() / loader / 宿主应用）
assembleApp(app, cfg)                    // 服务装配到现有 ctx（与站点插件共享 DI）
await runPhase(app, cfg, { phase: 'both' })
```

### 原则

- **默认装配** = 开箱即用（两阶段管线）
- **cordis.yml** = 声明式组装（站点插件 / 用户插件 / fetcher，顺序即依赖）
- **createApp / assembleApp 返回的 Context** = 完整 cordis 上下文，可自由注入/监听/扩展
- 想替换什么就替换什么，不想替换就全默认——**自由度在你手里**
