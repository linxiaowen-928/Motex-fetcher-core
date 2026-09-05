/**
 * 索引发现器：
 *   kind='static' → 直接用种子 URL；
 *   kind='index'  → 抓目录种子页，按 indexRule.linkRegex 提取章节 URL（相对路径拼绝对）；
 *   kind='site'   → 分派给站点处理器（src/sites 注册表；多级分类/分页/标题关键字过滤等站点逻辑在处理器里）。
 *
 * 两阶段支持：索引池 sink —— 处理器在发现过程中逐条 pushIndex(record)，
 * 索引阶段就把 {url,title,...} 全量落池（写入 cfg.indexFile），爬取阶段从池取，互不干扰。
 */
import { appendFile, mkdir } from 'node:fs/promises'
import { dirname, join } from 'node:path'
import { Context, Service } from '@deepseek-ai/cordis'
import type { SourceConfig } from '../config.ts'
import { decodeBytes, extractLinks } from '../rules.ts'
import type { SiteHandler } from '../types.ts'
import type { IndexRecord } from '../types.ts'

export class IndexerService extends Service {
  private indexFile: string | null = null

  constructor(ctx: Context) {
    super(ctx, 'indexer')
  }

  /** 开启索引池写入（phase=index/both 时由入口调用） */
  beginIndex(file: string) {
    this.indexFile = file
  }

  /** 处理器在发现过程中逐条追加进索引池（幂等，无需在内存攒全量） */
  async pushIndex(rec: IndexRecord): Promise<void> {
    if (!this.indexFile) return
    const file = join(process.cwd(), this.indexFile)
    await mkdir(join(process.cwd(), dirname(this.indexFile)), { recursive: true })
    await appendFile(file, JSON.stringify(rec) + '\n', 'utf-8')
  }

  /** 返回该源应当抓取的 URL 列表（site 处理器同时会把条目写入索引池） */
  async discover(source: SourceConfig): Promise<string[]> {
    if (source.kind === 'site') {
      // cordis DI 分派：站点处理器由项目插件 ctx.provide('site.<id>', handler) 注册
      // ⚠️ 装配竞态（2026-09-05 index 阶段暴露）：loader 并行 apply 条目——fetcher 的 runPhase
      //    可能与站点插件的 provide 同时进行，discover 时 handler 可能还没注册 → 等它出现
      const h = await this.waitSiteHandler(source.siteHandler)
      if (!h) {
        this.ctx.logger.error('[indexer] 未提供站点处理器 %s（项目需注册 cordis 服务 site.%s）', source.siteHandler, source.siteHandler)
        return []
      }
      return h.discover(this.sourceScopedCtx(source), source)
    }
    if (source.kind === 'static' || !source.indexRule) {
      return source.seedUrls
    }
    const out = new Set<string>()
    for (const seed of source.seedUrls) {
      try {
        const r = await this.ctx.scheduler.fetchFor(source.id, seed, 30000)
        if (!r.ok || !r.body) {
          this.ctx.logger.warn('[indexer] 目录页失败 %s (HTTP %s)', seed, r.status)
          continue
        }
        const html = decodeBytes(r.body, source.parseRule?.encoding)
        const linkRe = new RegExp(source.indexRule.linkRegex, 'g')
        for (const u of extractLinks(html, linkRe, seed)) {
          out.add(u)
        }
      } catch (e) {
        this.ctx.logger.warn('[indexer] 目录页异常 %s: %s', seed, String(e))
      }
    }
    this.ctx.logger.info('[indexer] 源 %s：目录发现 %d 个 URL', source.id, out.size)
    return [...out]
  }

  /** 给站点 handler 一个“按源路由”的子 ctx（2026-09-05 多源共享进程）：
   *  站点适配器的旁路抓取走 ctx.scheduler.client——单进程多源时必须按源走 transport 策略
   *  （curl/代理），否则拿错客户端全挂。适配器只用 scheduler.client + indexer.pushIndex +
   *  logger 三样（已全量核对），故只遮蔽 scheduler；pushIndex 经真实 indexer（池文件已由
   *  beginIndex 切到本源），logger 走原型链继承。
   *  ⚠️ scheduler 只给 client/push 两个成员——队列任务请勿经此 ctx 派发（fetchFor 是旁路）。 */
  private sourceScopedCtx(source: SourceConfig): Context {
    const real = this.ctx
    const sub = real.extend({}) as Context
    const sched = real.scheduler
    Object.defineProperty(sub, 'scheduler', {
      value: {
        client: (url: string, timeout?: number) => sched.fetchFor(source.id, url, timeout),
        push: (...args: Parameters<typeof sched.push>) => sched.push(...args),
      },
      configurable: false,
    })
    return sub
  }

  /** 等待站点处理器注册（loader 并行 apply 竞态兜底：轮询最多 timeoutMs）
   *  120s：站点插件冷加载可达 16-30s（strip-types 新进程全量编译 + 新文件），
   *  20s 窗口会在慢 import 时超时空手（2026-09-05 目标限流站 index 空手根因） */
  private async waitSiteHandler(id: string | undefined, timeoutMs = 120_000): Promise<SiteHandler | null> {
    const t0 = Date.now()
    while (Date.now() - t0 < timeoutMs) {
      try {
        const h = this.ctx.get('site.' + id)
        if (h && typeof (h as { discover?: unknown }).discover === 'function') {
          return h as SiteHandler
        }
      } catch { /* 服务未注册 → 继续等 */ }
      await new Promise((r) => setTimeout(r, 100))
    }
    return null
  }
}