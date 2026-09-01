# Motex-fetcher-core 架构说明

> 内部怎么组织的：模块职责、数据流、调度器工作机制、断点格式、日志事件。

## 一、模块地图

```
src/
├── index.ts              # 入口：解析参数 → 装配服务 → 两阶段主流程 → 自检
├── config.ts             # 配置契约（sources/scheduler/storage/manage）+ 默认值
├── types.ts              # 核心类型（FetchJob/FetchResponse/IndexRecord/ParsedItem）+ 事件契约
├── rules.ts              # 解析规则工具（decodeBytes/extractLinks/extractWithSelectors/分页检测）
├── trace.ts              # tlog 结构化日志（JSONL 追加、按站分文件）
├── services/
│   ├── scheduler.ts      # ★ 调度器（并发/重试/去重/断点/429 自适应）——核心中的核心
│   ├── indexer.ts        # 索引发现（static/index/site 三种 kind 分发）
│   ├── parser.ts         # 解析器（解码→选择器→广告过滤→分页拼接）
│   ├── storage.ts        # 存储（JSONL 分片落盘/done.urls/二进制保存/下载调度）
│   ├── proxy.ts          # 代理池轮换（可选）
│   └── manage.ts         # 管理服务（API + Web，可选）
├── plugins/pipeline.ts   # 事件接线（fetch/response → parse → storage）
├── discovery/
│   ├── registry.ts       # 站点处理器注册表（registerSiteHandler）
│   ├── bfs.ts            # 站内 BFS 发现通道（参数化）
│   └── update.ts         # 追更框架（ongoing 标记 + diff）
├── download_worker.ts    # 大文件下载 worker（独立进程：Range 分块/断点/认证）
└── cli.ts                # CLI 入口（node src/cli.ts --config ...）
```

## 二、两阶段数据流

```
main()
 ├─ phase = index|both
 │    ├─ indexer.discover(source)          # 按 kind 分发
 │    │    ├─ static: 直接返回 seedUrls
 │    │    ├─ index:  抓目录页 + linkRegex 提取
 │    │    └─ site:   调注册的 handler.discover
 │    └─ pushIndex → pool/<site>.index.jsonl   # 索引池落盘
 ├─ phase = crawl|both
 │    ├─ 读 pool → 内存 URL 列表
 │    ├─ skipExisting: done.urls 快路径过滤已爬
 │    ├─ downloadRaw 源 → 独立 worker 流式下载
 │    └─ scheduler.push(urls) → 队列
 ├─ scheduler.waitIdle()                    # 等全部终局
 └─ 兜底重试轮（failRetryPasses）
```

## 三、调度器工作机制

### 队列与并发

```
push(urls) → 内存队列（每 URL 一个 job：url/source/attempts/requeues）
     ↓
N 个 worker 并发取 job → scheduler.client(url) → 结果
     ↓
成功 → 派发 fetch/response（管线落盘）
失败 → 重试（指数退避）→ 仍失败 → 重入队（requeueDelayMs 指数升级，封顶 32x）
     → 达到 maxAttempts → 终局失败（fails.jsonl）
```

### 去重

- `visited` 集合：已访问 URL 不再重复抓（`dedupe: true`）
- 断点恢复时 visited 从 run.json 恢复

### 断点 checkpoint

- **时机**：每批 URL 队列清空时自动落盘（`checkpoint()`）
- **内容**：`{ queue, visited, failed, stats }` → state/<site>/run.json
- **恢复**：启动时 restore → 队列/去重/失败状态接续

### 429 自适应

- 60s 内命中 ≥5 次 429/503 → 并发临时降为 8（5 分钟）→ 心跳恢复

## 四、落盘与分片

```
正文：out/<site>.jsonl（每行 ParsedItem）
分片：单文件 ≥64MB 自动切 .part1/.part2/...（避免单文件过大）
已爬：out/<site>.done.urls（每行一个 url——重启快路径去重）
失败：out/<site>.fails.jsonl
池：  pool/<site>.index.jsonl
```

## 五、日志事件（state/<site>.log，JSONL）

| 事件 | 含义 |
|---|---|
| `start` | 进程启动（phase/sources） |
| `push` | 入队一批 URL（n/added/source） |
| `hb` | 心跳（30s：队列/在飞/并发/统计） |
| `req_end` | 一个请求完成（ok/status/ms） |
| `crawl_plan` | 本次计划抓取数 |
| `crawl_progress` | 每 2500 条进度（ok/failed/requeued） |
| `crawl_main_done` / `crawl_retry_done` | 主轮/兜底轮完成 |
| `pause_requested` / `paused_clean` | 优雅暂停（检测到 flag → checkpoint → 退出） |
| `manage_start` | 管理服务启动 |
| `end` | 收尾（总账） |

## 六、扩展机制总览

```
注册表 registry：registerSiteHandler({id, discover})
   ├─ config.sources[].siteHandler 指向注册的 id
   └─ indexer 按 id 分派

kind 扩展：static / index / site /（downloadRaw 修饰符）
   └─ 新增 kind = 在 indexer.ts 加分支（或复用 site handler）

事件钩子：fetch/response、fetch/parsed、fetch/failed
   └─ pipeline.ts 接线；可在项目里监听扩展（如统计/入库）
```

## 七、设计原则

1. **解耦**：调度器不知道解析器，解析器不知道存储——事件串起来（pipeline）
2. **可恢复**：一切断点/池/输出都是文件资产，随时续跑
3. **礼貌**：重试/退避/429 自适应内置，默认不暴力
4. **零个性化**：框架不认识任何具体站点；站点 = 项目注册的 handler