/**
 * fetcher 插件：以 cordis 插件形式把抓取器装进任意 cordis 应用。
 *
 * 用法（配合 loader.ts 的 cordis.yml）：
 * ```yaml
 * - id: fetcher
 *   name: 'cordis:fetcher'            # 或 'motex-fetcher-core/fetcher'（经 node_modules 解析）
 *   config:
 *     config: './config.json'         # 抓取器配置文件（结构见 config.ts）
 *     phase: 'both'                   # 可选：index / crawl / both / update
 *     limit: 0                        # 可选：本次最多抓取 URL 数（0 = 不限）
 *     concurrency: 0                  # 可选：覆盖调度并发（>0 生效）
 *     watch: true                     # 可选：首轮结束后常驻，热接入新站点（见下）
 * ```
 *
 * ## watch 常驻模式（单进程多源；2026-09-05 架构修正——Option A）
 *
 * 配合 loader 的 HMR（cordis.yml 文件监听热更新）：
 * 1. 装配文件的 fetcher 条目带 `watch: true`：apply 先跑一轮初始 runPhase，
 *    之后两个常驻循环（detached fiber，互不阻塞）：
 *    a. 【常驻发现循环】：每源按自己的 indexIntervalSec（缺省 3600s）跑一轮 discover →
 *       增量落池（site 类由 handler pushIndex；静态类返回 URL 直接补池）→ 有新条目则 poke 扫池。
 *       ——替代独立 index 阶段进程（index_watch 退役）；发现是旁路抓取（不走调度队列），
 *       与正文爬并行；每源传输策略（curl/代理）与正文同一套（scheduler.fetchFor 按源路由）。
 *    b. 【常驻扫池循环】：每 refreshSec（缺省 600s）按源 loadIndex（源级 indexFile，
 *       多源每源自己的池文件）→ skipExisting(done.urls) → force 补推新条目 → 落定。
 *       ——等价于标准进程"重启一轮"，无需重启即实现追更/续爬。
 *    调度器多源公平派发（窗口扫描取在飞最少且限速就绪的源），大池源不会饿死小池源。
 * 2. 热接入新站点：往 cordis.yml 追加站点条目（config 带 `source` 字段：id/outDir/indexFile/
 *    transport/indexIntervalSec/parseRule…）→ HMR → 插件 apply → provide site.<id> +
 *    emit source/register → fetcher：源入 cfg.sources + 立即发现一轮（新站先建池）+ 扫池。
 * 3. 多源共享同一 fetcher：各源声明自己的 outDir / indexFile / transport / indexIntervalSec。
 * 4. 暂停：`state/pause_crawls.flag` → checkpoint 落盘 → 干净退出（在跑的发现轮随进程退出
 *    中断——handler 增量幂等 + 池只增补，重跑无损）；Ctrl+C 同理。
 *
 * 关键点：服务装配在【当前 ctx】上（assembleApp），与同一上下文树里的站点插件共享
 * cordis 作用域 DI 与事件——indexer 通过 ctx.get('site.<id>') 分派站点处理器，
 * 用户插件可直接监听 fetch/response 等事件、注入/替换核心服务。
 */
import { Context } from '@deepseek-ai/cordis'
import { existsSync } from 'node:fs'
import { resolve } from 'node:path'
import { fileURLToPath } from 'node:url'
import { loadConfig, type FetcherConfig, type SourceConfig } from './config.ts'
import { assembleApp, prepareSelfTest, runCrawlSweep, runIndexSweep, runPhase, type FetchCoreOptions } from './index.ts'

export interface FetcherPluginConfig {
  /** 抓取器配置文件（相对运行目录的 JSON；结构见 src/config.ts） */
  config?: string
  /** 阶段：index / crawl / both / update（缺省取配置文件 phase，再缺省 both） */
  phase?: 'index' | 'crawl' | 'both' | 'update'
  /** 本次最多抓取的 URL 数（0 = 不限） */
  limit?: number
  /** 覆盖调度并发（>0 生效） */
  concurrency?: number
  /** 自检模式：假客户端 + 隔离目录 + 断言输出（验证 cordis 装配链路） */
  selfTest?: boolean
  /** watch 常驻：首轮结束后常驻扫池续爬，热接入新站点（配 loader watch 模式使用） */
  watch?: boolean
  /** watch 扫池周期（秒，缺省 600=10 分钟）：周期 loadIndex → force 补推新池条目（追更） */
  refreshSec?: number
  /** watch 发现轮周期（秒，缺省 3600；每源可用 indexIntervalSec 覆盖）：周期 discover → 池落盘 */
  indexIntervalSec?: number
  /** 核心装配选项透传（替换服务/注册插件/钩子），一般项目不需要 */
  core?: FetchCoreOptions
}

export const fetcherPlugin = {
  name: 'motex-fetcher-core/fetcher',
  apply: async (ctx: Context, config: FetcherPluginConfig = {}) => {
    const cfg = loadConfig(resolveConfigPath(ctx, config.config))
    if (!cfg.sources.length && !config.selfTest && !config.watch) {
      throw new Error(`[motex-fetcher-core/fetcher] 未配置 sources（config: ${config.config ?? '(未指定，用 config 字段指向抓取器配置文件)'}）`)
    }
    if (config.selfTest) prepareSelfTest(cfg)
    // 服务装配到当前 ctx（loader 的上下文树）：站点插件（同树提供 site.<id>）经 cordis DI 被 indexer 分派
    assembleApp(ctx, cfg, config.core ?? {})
    const result = await runPhase(ctx, cfg, {
      phase: config.phase,
      limit: config.limit ?? 0,
      concurrency: config.concurrency ?? 0,
      selfTest: config.selfTest,
    })
    // 暂停 = 干净结束：checkpoint 已落盘，apply 正常返回（CLI 侧检查 scheduler.isPaused() 决定退出）
    if (result.paused) {
      ctx.logger.warn('[motex-fetcher-core/fetcher] 已按暂停标记优雅退出（checkpoint 已落盘）')
      return
    }
    // watch 常驻：apply 正常返回（不阻塞装载——否则 loader 的 create() 挂起、文件监听永不启动）。
    // 进程常驻由 loader 的文件监听（--watch）维持；监听器随本 fiber 生命周期存活（HMR 卸载时自动清理）；
    // 暂停：pauseIv 发 pause/clean → CLI 侧退出（checkpoint 已落盘）。
    if (config.watch) {
      const refreshSec = config.refreshSec ?? 600
      const indexIntervalSec = config.indexIntervalSec ?? 3600
      ctx.logger.info(
        '[fetcher] watch 常驻：扫池续爬（%ss）+ 发现轮（每源 %ss）+ source/register 热接入；pause flag 或 Ctrl+C 干净退出',
        refreshSec, indexIntervalSec)
      const started = new Set(cfg.sources.map((s) => s.id))
      const crawlWake = new Wake()
      const indexWake = new Wake()
      ctx.on('source/register', (src: SourceConfig) => {
        if (!src?.id || started.has(src.id)) return
        started.add(src.id)
        if (!cfg.sources.some((s) => s.id === src.id)) {
          cfg.sources.push(src)
          ctx.storage.registerSourceDirs([src])   // 源级 outDir（热接入晚于服务装配）
          ctx.scheduler.registerSources([src])    // 源级传输策略（curl/代理/节奏）
        }
        ctx.logger.info('[fetcher] 热接入新源 %s（kind=%s）：入发现+扫池循环', src.id, src.kind)
        indexWake.poke()    // 立即发现一轮（新站先有池，扫池才有货）
        crawlWake.poke()
      })
      const phase = (config.phase ?? cfg.phase ?? 'both') as 'index' | 'crawl' | 'both' | 'update'
      const wantCrawl = phase === 'crawl' || phase === 'both'
      const wantIndex = phase === 'index' || phase === 'both'
      // detached fibers：不能 await（loader.await 会等 apply 内所有任务落定 → 永不落定挂死）
      if (wantCrawl) {
        void (async () => {
          for (;;) {
            if (ctx.scheduler.isPaused()) return
            await crawlWake.wait(refreshSec * 1000)
            if (ctx.scheduler.isPaused()) return
            try {
              await runCrawlSweep(ctx, cfg, { limit: config.limit ?? 0, selfTest: config.selfTest })
            } catch (e) {
              ctx.logger.error('[fetcher] 扫池一轮失败: %s', String(e).slice(0, 160))
            }
          }
        })()
      }
      if (wantIndex) {
        // 常驻发现循环（2026-09-05 index 并入单进程）：每源按自己的 indexIntervalSec 周期跑 discover
        // （旁路抓取，与正文爬并行不抢队列）；热接入新源 → poke 立即发现；发现补池后 poke 扫池。
        const last = new Map<string, number>()
        const running = new Set<string>()
        void (async () => {
          const pollMs = 30_000
          for (;;) {
            if (ctx.scheduler.isPaused()) return
            const now = Date.now()
            const due: SourceConfig[] = []
            for (const src of cfg.sources) {
              if (running.has(src.id)) continue
              const interval = (src.indexIntervalSec ?? indexIntervalSec) * 1000
              if ((last.get(src.id) ?? 0) + interval <= now) due.push(src)
            }
            if (due.length) {
              const results = await Promise.allSettled(due.map(async (src) => {
                running.add(src.id)
                last.set(src.id, Date.now())
                try {
                  const n = await runIndexSweep(ctx, cfg, src)
                  if (n > 0 && wantCrawl) crawlWake.poke()    // 有新池 → 让扫池马上消化
                  return { src: src.id, n }
                } finally {
                  running.delete(src.id)
                }
              }))
              for (const r of results) {
                if (r.status === 'fulfilled') ctx.logger.info('[fetcher] 发现轮 %s 完成：%d 条', r.value.src, r.value.n)
                else ctx.logger.error('[fetcher] 发现轮失败: %s', String(r.reason).slice(0, 160))
              }
            }
            if (ctx.scheduler.isPaused()) return
            await indexWake.wait(pollMs)
          }
        })()
      }
    }
  },
}

/** 唤醒闸：poke 置位（含等待期间未消费的 poke——多源连续热接入不丢）；wait 先查置位再起定时。 */
class Wake {
  private armed = false
  private fn: (() => void) | null = null
  private timer: ReturnType<typeof setTimeout> | null = null

  poke(): void {
    if (this.fn) {
      const fn = this.fn
      this.fn = null
      if (this.timer) clearTimeout(this.timer)
      this.timer = null
      fn()
    } else {
      this.armed = true
    }
  }

  async wait(ms: number): Promise<void> {
    if (this.armed) {
      this.armed = false
      return
    }
    await new Promise<void>((resolve) => {
      this.timer = setTimeout(() => {
        this.timer = null
        this.fn = null
        resolve()
      }, ms)
      this.fn = () => {
        this.timer = null
        this.fn = null
        resolve()
      }
    })
  }
}

export default fetcherPlugin

/** 配置路径解析（DSH 语义）：先按 cordis.yml 所在目录，再按运行目录（CWD）兜底；
 *  绝对路径原样返回；程序化使用（无 baseUrl）时只走 CWD。 */
function resolveConfigPath(ctx: Context, p?: string): string | undefined {
  if (!p) return undefined
  if (ctx.baseUrl) {
    try {
      const fromYml = fileURLToPath(new URL(p, ctx.baseUrl))
      if (existsSync(fromYml)) return fromYml
    } catch { /* 非法 URL 忽略，走 CWD */ }
  }
  return resolve(process.cwd(), p)
}
