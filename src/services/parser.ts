/**
 * 正文解析器（站点实例化后的解析执行器）：
 * 按 SourceConfig.parseRule（SiteParseRule）完成：
 *   解码(utf-8/gbk) → 选择器(section/title/content)或锚串式 → 行级广告过滤 → 最小长度 → 标题。
 *
 * 内容分页跟随（followPagination=true，防御能力）：
 *   首页解析后检测“下一页”链接 → 派发续页任务（continuationOf=基准URL）→ 缓冲暂存；
 *   续页同样处理；末页无下一页时把多页正文【拼成一条】落盘（url=基准URL）。
 *   实测 wx 站 40/40 无分页，该能力默认按站点配置开关。
 */
import { Context, Service } from '@deepseek-ai/cordis'
import * as cheerio from 'cheerio'
import type { SourceConfig } from '../config.ts'
import {
  decodeBytes, detectNextPage, extractText, extractWithSelectors, normalizeBase,
} from '../rules.ts'
import type { FetchResponse, ParsedItem } from '../types.ts'

interface PaginationBuffer {
  base: string
  source: string
  title?: string
  parts: string[]
}

export class ParserService extends Service {
  private buffers = new Map<string, PaginationBuffer>()

  constructor(ctx: Context) {
    super(ctx, 'parser')
  }

  /** 解析一次抓取结果 → 正文条目列表（分页拼接时中间页返回空，末页输出合并条目） */
  parse(res: FetchResponse, source: SourceConfig): ParsedItem[] {
    if (!res.ok || !res.body) return []
    const rule = source.parseRule
    const html = decodeBytes(res.body, rule?.encoding)

    // ---- 详情页信号：解析章节链接并入队（此页面本身不入库；章节页再走正文提取） ----
    if (rule?.chapterSignal && html.includes(rule.chapterSignal)) {
      const $ = cheerio.load(html)
      const prefix = rule.chapterLinkPrefix ?? ''
      const urls: string[] = []
      const cur = new URL(res.url)
      $('a[href]').each((_, el) => {
        const href = $(el).attr('href') ?? ''
        if (prefix && !href.includes(prefix)) return
        try {
          const abs = new URL(href, cur).href
          if (abs !== res.url) urls.push(abs)      // 剔除自身回链
        } catch { /* 忽略坏链接 */ }
      })
      if (urls.length) {
        this.ctx.logger.info('[parser] 详情页展开 %d 个章节 -> %s', urls.length, res.url)
        void this.recordChapters(source.id, res.url, urls)   // 章节登记表（追更 diff 用）
        void this.ctx.scheduler.push(urls, source.id, 0, { front: true })  // 章节插队：立即执行，不等详情清完
      }
      return []
    }

    // ---- 续页：拼进缓冲区 ----
    if (res.continuationOf) {
      const buf = this.buffers.get(res.continuationOf)
      if (!buf) {
        this.ctx.logger.warn('[parser] 续页无主缓冲（可能起点页失败），丢弃 %s', res.url)
        return []
      }
      const hit = this.extract(html, rule)
      if (!hit) return []
      buf.parts.push(hit.text)
      if (rule?.followPagination) {
        const next = detectNextPage(html, rule.section, res.url)
        if (next) {
          this.ctx.logger.debug('[parser] 续页跟随 %s → %s', res.url, next)
          void this.ctx.scheduler.push([next], source.id, 0, { continuationOf: buf.base })
          return []
        }
      }
      this.buffers.delete(buf.base)
      this.ctx.logger.info('[parser] 分页拼接完成：%s（%d 页）', buf.base, buf.parts.length)
      return [{
        url: buf.base, source: source.id,
        text: buf.parts.join('\n'),
        title: buf.title ?? hit.title,
        meta: { joinedPages: buf.parts.length },
      }]
    }

    // ---- 普通页（含可能的首页） ----
    const hit = this.extract(html, rule)
    if (!hit) return []
    if (rule?.followPagination) {
      const base = normalizeBase(res.url)
      const next = detectNextPage(html, rule.section, res.url)
      if (next && next !== res.url) {
        this.buffers.set(base, { base, source: source.id, title: hit.title, parts: [hit.text] })
        this.ctx.logger.debug('[parser] 检测到分页：%s → %s（进缓冲）', res.url, next)
        void this.ctx.scheduler.push([next], source.id, 0, { continuationOf: base })
        return []
      }
    }
    return [{
      url: res.url, source: source.id, text: hit.text,
      title: hit.title ?? titleFromHtml(html),
      meta: { encoding: rule?.encoding ?? 'utf-8' },
    }]
  }

  /** 章节登记表：state/<source>_chapters.jsonl 追加 {novelUrl, chapters[], at}（追更 diff 用） */
  private async recordChapters(source: string, novelUrl: string, chapters: string[]) {
    try {
      const { appendFile, mkdir } = await import('node:fs/promises')
      const { join } = await import('node:path')
      const file = join(process.cwd(), 'state', `${source}_chapters.jsonl`)
      await mkdir(join(process.cwd(), 'state'), { recursive: true })
      await appendFile(file, JSON.stringify({ novelUrl, chapters, at: new Date().toISOString() }) + '\n', 'utf-8')
    } catch { /* 登记失败不影响抓取 */ }
  }

  /** 选择器式 / 锚串式 / 无规则基础清洗 三选一 */
  private extract(html: string, rule?: SourceConfig['parseRule']) {
    if (rule?.section) return extractWithSelectors(html, rule)
    if (rule) return extractText(html, rule)
    const text = html
      .replace(/<script[\s\S]*?<\/script>/gi, ' ')
      .replace(/<style[\s\S]*?<\/style>/gi, ' ')
      .replace(/<[^>]+>/g, ' ')
      .replace(/&nbsp;/g, ' ').replace(/&lt;/g, '<').replace(/&gt;/g, '>')
      .replace(/&amp;/g, '&').replace(/&quot;/g, '"')
      .replace(/[ \t]+/g, ' ')
      .replace(/\n{3,}/g, '\n\n')
      .trim()
    if (text.length < 40) return null
    return { text }
  }
}

/** 标题兜底：<title> 文本，取 “ - ” 前第一段（去站点后缀） */
function titleFromHtml(html: string): string | undefined {
  const m = /<title>([^<]+)<\/title>/i.exec(html)
  if (!m) return undefined
  const first = m[1].trim().split(' - ')[0].trim()
  return first || undefined
}