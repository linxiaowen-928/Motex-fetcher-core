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
 * ## watch 常驻模式（运行中热接入新站点）
 *
 * 配合 loader 的 HMR（cordis.yml 文件监听热更新）：
 * 1. 运行中的进程监听 `source/register` 事件（站点插件 apply 时 emit）
 * 2. 往 cordis.yml 追加一个站点条目（config 里带 `source` 字段）→ 热更新 → 插件 apply
 *    → `ctx.provide('site.<id>', handler)` + `ctx.emit('source/register', sourceConfig)`
 * 3. fetcher 收到事件 → 自动为该源跑一轮（发现 → 入队 → 落定），老源不受影响
 * 4. 暂停：`state/pause_crawls.flag` → checkpoint 落盘 → 干净退出；Ctrl+C 同理
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
import { assembleApp, prepareSelfTest, runPhase, type FetchCoreOptions } from './index.ts'

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
  /** watch 常驻：首轮结束后监听 source/register，热接入新站点（配 loader watch 模式使用） */
  watch?: boolean
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
      ctx.logger.info('[fetcher] watch 常驻：监听 source/register 热接入新站点；pause flag 或 Ctrl+C 干净退出')
      const started = new Set(cfg.sources.map((s) => s.id))
      ctx.on('source/register', (src: SourceConfig) => {
        if (!src?.id || started.has(src.id)) return
        started.add(src.id)
        if (!cfg.sources.some((s) => s.id === src.id)) cfg.sources.push(src)
        ctx.logger.info('[fetcher] 热接入新源 %s（kind=%s）', src.id, src.kind)
        void runSourceOnce(ctx, cfg, src).catch((e) =>
          ctx.logger.error('[fetcher] 新源 %s 一轮失败: %s', src.id, String(e).slice(0, 160)))
      })
    }
  },
}

/** 单源一轮（watch 热接入用）：发现 → 入队 → 落定。
 *  去重/重试/断点由调度器负责；进程常驻，visited 集合持续有效（重启后由 done.urls 兜底）。 */
async function runSourceOnce(app: Context, cfg: FetcherConfig, src: SourceConfig) {
  // 开索引池：discover 内 pushIndex 才能落盘（2026-09-05：漏开导致池不落盘，
  // 重启后 seen 空 → 每轮全量重发现）
  app.indexer.beginIndex(cfg.indexFile ?? `pool/${src.id}.index.jsonl`)
  const urls = await app.indexer.discover(src)
  if (!urls.length) {
    app.logger.info('[fetcher] 新源 %s：无 URL', src.id)
    return
  }
  await app.scheduler.push(urls, src.id, 0)
  await app.scheduler.waitIdle()
  app.logger.info('[fetcher] 新源 %s 一轮完成：ok=%d failed=%d', src.id, app.scheduler.stats.ok, app.scheduler.stats.failed)
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
