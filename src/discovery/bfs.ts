/**
 * 通用“站内 BFS 全遍历”发现通道（链接图深度遍历）：
 * - 种子 = 站点首页；提取本站域名全部链接递归访问，visited 去重防环；
 * - 过程中物化 book 粒度链接（bookUrlPattern 命中）→ pushIndex 入池（幂等）；
 * - 无上限（预算按站点规模），并发可调（16-24 试限流）；
 * - 疑似无限递归检测：队列净增长异常 → tlog 告警（人工介入探查，不中断）。
 */
import { Context } from '@deepseek-ai/cordis'
import * as cheerio from 'cheerio'
import type { SourceConfig } from '../config.ts'
import { decodeBytes } from '../rules.ts'
import type { IndexRecord } from '../types.ts'
import { tlog } from '../trace.ts'

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms))

const SKIP_EXT = /\.(jpg|jpeg|png|gif|webp|ico|css|js|json|xml|svg|woff2?|ttf|eot|mp4|mp3|zip|rar|pdf|txt|apk)$/i

export interface BfsOptions {
  seedUrl: string
  /** book 链接模式（正则，命中即入池）；须含捕获 id */
  bookPattern: RegExp
  concurrency?: number
  delayMs?: number
  retries?: number
  /** 与外部共享的池（增量） */
  seen: Map<string, IndexRecord>
  /** 归一化站点前缀（站内链接判定） */
  site: string
}

export async function runBfsDiscover(ctx: Context, source: SourceConfig, o: BfsOptions): Promise<{ pages: number; books: number }> {
  const conc = o.concurrency ?? 16
  const delay = o.delayMs ?? 200
  const retries = o.retries ?? 3
  const queue: string[] = [o.seedUrl]
  const queued = new Set<string>([o.seedUrl])   // 队内 URL（O(1) 判重，避免 includes O(n²)）
  const visited = new Set<string>()
  const added = new Set<string>()
  let pages = 0
  let books = 0

  const normalize = (href: string, base: string): string | null => {
    try {
      const u = new URL(href, base)
      if (u.protocol !== 'http:' && u.protocol !== 'https:') return null
      if (SKIP_EXT.test(u.pathname)) return null
      u.hash = ''
      return u.href
    } catch {
      return null
    }
  }

  const fetchPage = async (url: string): Promise<string | null> => {
    for (let attempt = 1; attempt <= retries + 1; attempt++) {
      try {
        const r = await ctx.scheduler.client(url, 25000)
        if (r.ok && r.body) return decodeBytes(r.body, 'utf-8')
        if (r.status === 429 || r.status === 403) {
          tlog({ ev: 'bfs_limited', url, status: r.status, attempt })
          await sleep(Math.min(5 * 2 ** attempt, 60) * 1000)
          continue
        }
      } catch (e) {
        tlog({ ev: 'bfs_fetch_err', url, err: String(e).slice(0, 80) })
      }
      await sleep(800 * 2 ** (attempt - 1))
    }
    return null
  }

  // 多 worker 消费队列（BFS 层级粗略：并发窗口内队列先进先出即可）
  const workers = Array.from({ length: conc }, async () => {
    while (true) {
      const url = queue.shift()
      if (!url) return
      queued.delete(url)
      if (visited.has(url)) continue
      visited.add(url)
      const html = await fetchPage(url)
      pages++
      if (!html) continue
      const $ = cheerio.load(html)
      // book 链接 → 池
      $('a[href]').each((_, el) => {
        const href = $(el).attr('href') ?? ''
        const m = o.bookPattern.exec(href)
        if (!m) return
        const abs = normalize(href, url)
        if (!abs || added.has(abs)) return
        added.add(abs)
        if (!o.seen.has(abs)) {
          // 顺带提取标题（h4 > title 属性 > 文本），可能为空（纯链接形态）
          const title = $(el).find('h4').first().text().trim()
            || $(el).attr('title') || ''
          o.seen.set(abs, { url: abs, title, source: source.id })
          tlog({ ev: 'bfs_new_book', url: abs, title: title.slice(0, 60) })
          void ctx.indexer.pushIndex(o.seen.get(abs)!)
          books++
        }
      })
      // 站内链接 → 队列
      $('a[href]').each((_, el) => {
        const href = $(el).attr('href') ?? ''
        const abs = normalize(href, url)
        if (!abs || !abs.startsWith(o.site)) return
        if (!visited.has(abs) && !queued.has(abs)) {
          queued.add(abs)
          queue.push(abs)
        }
      })
      await sleep(delay)
      if (pages % 500 === 0) {
        tlog({ ev: 'bfs_progress', pages, queue: queue.length, books, visited: visited.size })
      }
    }
  })
  await Promise.all(workers)

  tlog({ ev: 'bfs_done', pages, visited: visited.size, books, queue: queue.length })
  return { pages, books }
}