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
      const h = source.siteHandler ? this.ctx.get('site.' + source.siteHandler) : undefined
      if (!h || typeof (h as any).discover !== 'function') {
        this.ctx.logger.error('[indexer] 未提供站点处理器 %s（项目需注册 cordis 服务 site.%s）', source.siteHandler, source.siteHandler)
        return []
      }
      return (h as SiteHandler).discover(this.ctx, source)
    }
    if (source.kind === 'static' || !source.indexRule) {
      return source.seedUrls
    }
    const out = new Set<string>()
    for (const seed of source.seedUrls) {
      try {
        const r = await this.ctx.scheduler.client(seed, 30000)
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
}