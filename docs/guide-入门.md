# Motex-fetcher-core 入门指南

> 从零理解这个框架怎么工作，并用最小配置跑通第一个抓取任务。

## 一、这个框架解决什么问题

你要从网站抓数据，常见诉求：

- 从**列表页**发现一堆**详情页**，然后抓每个详情页的正文
- 数据量大，抓一半断了，**下次接着抓**（不重复、不丢失）
- 目标网站慢/限流，需要**控制并发和重试**
- 抓下来的内容**规整落盘**，还要能随时暂停/恢复

本框架把这件事抽象成**两阶段管线**，你只需要：**写配置**（抓什么）+ **写站点规则**（怎么解析）。

## 二、核心心智模型：两阶段（index → crawl）

用生活比喻：**逛书店进货**

```
阶段一 index（发现）：拿一份"书店地址清单"，挨家进去看有哪些书，把书名记到本子上
        → 产出：索引池 pool/（book 清单）

阶段二 crawl（抓取）：照着本子，挨家去把书搬回来
        → 产出：正文 out/*.jsonl（每行一条 JSON）

断点（checkpoint）：每次搬完一批货，在记账本上划掉 → 断电了下次从没划掉的地方继续
```

```
[配置 sources] ──index──▶ [索引池 pool] ──crawl──▶ [正文 out]
                        （URL 清单）              （JSONL 落盘）
                              ▲                      │
                              └──── 断点 run.json ────┘
```

- `--phase both`：一次跑完两个阶段
- `--phase index` / `--phase crawl`：分开跑（先建池，后抓取——适合大任务分步）

## 三、三种数据源（kind）

| kind | 干什么 | 例子 |
|---|---|---|
| `static` | 直接给 URL 列表 | `seedUrls: ["https://a.com/1"]`——固定页面 |
| `index` | 抓一个目录页，按正则提取链接 | 列表页 → 详情页链接 |
| `site` | 完全自定义发现逻辑（写代码） | 复杂站（分页/分类/去重）用 handler |

## 四、最小配置逐字段讲解

```jsonc
{
  "sources": [{
    "id": "demo",                  // 源标识（产物文件名、日志、断点都按它分）
    "kind": "static",              // 源类型（见上表）
    "seedUrls": ["https://example.com/"],  // 起始 URL
    "parseRule": {                 // 正文解析规则
      "encoding": "utf-8",         // 页面编码（gbk 站用 "gbk"）
      "section": "body",           // 从哪个容器提取（cheerio 选择器）
      "contentSelector": "p",      // 正文元素选择器（<p> 段落）
      "adLineRegex": ["www\\."],   // 广告行过滤（正则列表，命中剔除）
      "minLen": 20                 // 正文最小长度（太短丢弃）
    }
  }],
  "scheduler": {                   // 调度器
    "concurrency": 2,              // 并发连接数（2 很礼貌）
    "retries": 2,                  // 单 URL 重试次数
    "requeueDelayMs": 8000,        // 失败重入队延迟（限速礼貌）
    "maxAttempts": 5,              // 单 URL 总尝试上限（超过进 fails）
    "dedupe": true,                // 去重（已访问 URL 不重复抓）
    "stateFile": "state/demo/run.json"  // 断点文件
  },
  "storage": { "outDir": "out" }   // 输出目录
}
```

## 五、跑起来（两种方式）

**方式 A：CLI（不写代码）**
```bash
node --experimental-strip-types core/src/cli.ts --config ./config.json --phase both
```

**方式 B：作为库（适合要扩展的时候）**
```ts
// run.ts
import './my-handler.ts'                    // 注册你的站点（可选）
import { main } from './core/src/index.ts'
await main(['--config', './config.json', '--phase', 'both'])
```
```bash
node --experimental-strip-types run.ts
```

## 六、跑完看什么（产物与日志）

```
out/demo.jsonl            # 正文：每行 {"url":..., "source":..., "text":...}
out/demo.done.urls        # 已爬记录（重启时快路径去重，不用重扫）
out/demo.fails.jsonl      # 终局失败清单（可审计/重试）
pool/demo.index.jsonl     # 索引池（index 阶段产物）
state/demo/run.json       # 断点（队列/已访问/失败快照）
state/demo.log            # 结构化日志：hb 心跳 / req_end 请求完成 / push 入队 ...
```

## 七、常见疑问

**Q: 抓一半断了怎么办？**
直接重跑同一条命令——`run.json` 断点恢复 + `done.urls` 去重，已抓的不重复，没抓的继续。

**Q: 为什么进程跑完不退出？（旧版本）**
pause 检测定时器未释放——已修复（`unref()`）。现在完成即退出。

**Q: 目标站很慢/429 限流？**
调小 `concurrency`、调大 `requeueDelayMs`；调度器自带 429 自适应降并发。

**Q: 抓下来是乱码？**
`parseRule.encoding` 设为页面实际编码（gbk 站常见）。

**下一步**：读《扩展开发指南》（写自己的站点 handler、BFS、下载源、管理服务）。