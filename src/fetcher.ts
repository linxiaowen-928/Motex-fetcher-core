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
 * ```
 *
 * 关键点：服务装配在【当前 ctx】上（assembleApp），与同一上下文树里的站点插件共享
 * cordis 作用域 DI 与事件——indexer 通过 ctx.get('site.<id>') 分派站点处理器，
 * 用户插件可直接监听 fetch/response 等事件、注入/替换核心服务。
 */
import { Context } from '@deepseek-ai/cordis'
import { existsSync } from 'node:fs'
import { resolve } from 'node:path'
import { fileURLToPath } from 'node:url'
import { loadConfig } from './config.ts'
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
  /** 核心装配选项透传（替换服务/注册插件/钩子），一般项目不需要 */
  core?: FetchCoreOptions
}

export const fetcherPlugin = {
  name: 'motex-fetcher-core/fetcher',
  apply: async (ctx: Context, config: FetcherPluginConfig = {}) => {
    const cfg = loadConfig(resolveConfigPath(ctx, config.config))
    if (!cfg.sources.length && !config.selfTest) {
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
    if (result.paused) ctx.logger.warn('[motex-fetcher-core/fetcher] 已按暂停标记优雅退出（checkpoint 已落盘）')
  },
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
