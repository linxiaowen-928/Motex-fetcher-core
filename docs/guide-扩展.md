# Motex-fetcher-core 扩展开发指南

> 学会写自己的站点处理器、发现逻辑、下载源，并开启管理服务。

## 一、扩展点总览

| 扩展点 | 机制 | 适用 |
|---|---|---|
| 站点处理器 | `registerSiteHandler({id, discover})` | 自定义"发现 URL"逻辑（分页/分类/去重） |
| 解析规则 | config 的 `parseRule` | 详情页正文提取（无需写代码） |
| 索引源 | `kind: 'index'` + `indexRule.linkRegex` | 目录页正则提取链接 |
| 发现通道 | `discovery/bfs.ts`（站内 BFS） | 全站遍历（不依赖列表页结构） |
| 追更 | `discovery/update.ts`（ongoing 标记） | 连载内容增量更新 |
| 下载源 | `downloadRaw: true` | 二进制大文件（音频/压缩包） |
| 管理服务 | config 的 `manage` | 浏览器/API 看状态、暂停/恢复 |

## 二、站点处理器（最常用扩展）

### 生命周期

```
config.sources 里声明 { "kind": "site", "siteHandler": "my-site" }
        ↓
核心调用你注册的 discover(ctx, source)
        ↓
你返回 URL 列表（可同时 pushIndex 入池）
        ↓
核心把这些 URL 交给 crawl 阶段（并发/重试/断点自动）
        ↓
每个 URL 抓下来 → 按 parseRule 解析 → 落盘
```

### 最小示例

```ts
// my-handler.ts
import { registerSiteHandler } from './core/src/discovery/registry.ts'
import { extractLinks } from './core/src/rules.ts'

registerSiteHandler({
  id: 'my-site',
  discover: async (ctx, source) => {
    // 1. 抓种子页（scheduler.client 走并发/重试/代理）
    const res = await ctx.scheduler.client(source.seedUrls[0], 20000)
    if (!res.ok || !res.body) return []
    const html = new TextDecoder().decode(res.body)
    // 2. 提取链接（extractLinks 自动拼绝对 URL）
    return extractLinks(html, /href="(\/detail\/\d+\.html)"/g, source.seedUrls[0])
  },
})
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

## 三、解析规则 parseRule 全字段

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

## 四、二进制下载源（音频/压缩包）

```jsonc
{ "id": "audio", "kind": "index", "downloadRaw": true,
  "seedUrls": ["https://example.com/files/"],
  "indexRule": { "linkRegex": "href=\"([^\"]+\\.tar\\.gz)\"" } }
```

- 抓到的文件不进内存，**流式落盘**（独立 worker 进程，Range 分块抗慢网）
- 断点续传：完成一个记一个（`data_audio/<source>.meta.jsonl`）
- 认证：worker 支持 `authHeaders`（如 `{ "Authorization": "Bearer xxx" }`）或 `hfToken`（兼容）

## 五、管理服务（可选开启）

```jsonc
{ "manage": { "enabled": true, "port": 8787, "api": true, "web": true } }
```

- `GET /api/status`：任务状态（输出/心跳/暂停态）
- `POST /api/pause`：优雅暂停（写 flag → 任务 checkpoint 干净退出）
- `POST /api/resume`：恢复（看护自动拉起）
- `GET /`：浏览器管理页（自动刷新）

## 六、优雅暂停机制（了解即可）

```
crawl_ctl 或管理服务 → 写 state/pause_crawls.flag
        ↓
运行中的任务每 5s 检测 flag → checkpoint 落盘 → 干净退出
        ↓
删除 flag → 看护器/手动重新拉起 → 断点续跑
```

## 七、最佳实践

1. **发现幂等**：discover 返回的 URL 重复没关系（scheduler 去重 + done.urls 兜底）
2. **礼貌限速**：慢站用 `concurrency: 2` + `requeueDelayMs: 8000` 起步
3. **验证一次**：先 `--limit 5` 小批跑通，再看 out/ 产物确认解析正确
4. **大文件源**：用 `downloadRaw` + 独立 worker（不要走正文管线）
5. **断点是资产**：`state/`、`pool/`、`out/` 都是可恢复资产，别随手删

**下一步**：读《架构说明》（调度器内部/断点格式/事件日志）。