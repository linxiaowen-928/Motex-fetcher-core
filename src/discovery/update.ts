/**
 * 追更（--phase update）：对连载中的小说（池内 ongoing=true）回访详情页，
 * diff 最新章节全集 vs 已抓章节（out/<site>.jsonl 的 url 集），只增量抓新章节。
 * 低并发 + 间隔（默认每本 ≥1.5s），并尊重站点的更新页逻辑（后续可按站优化）。
 */
import { Context } from '@deepseek-ai/cordis'
import * as cheerio from 'cheerio'
import { existsSync, readFileSync } from 'node:fs'
import { join } from 'node:path'
import { decodeBytes } from '../rules.ts'
import { tlog } from '../trace.ts'
import type { IndexRecord } from '../types.ts'
import type { SourceConfig } from '../config.ts'

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms))

export async function runUpdate(
  ctx: Context,
  source: SourceConfig,
  poolFile: string,
  concurrency = 2,
  delayMs = 1500,
): Promise<number> {
  // 1) 连载池
  const ongoing: IndexRecord[] = []
  if (!existsSync(poolFile)) {
    ctx.logger.warn('[update] 池不存在 %s', poolFile)
    return 0
  }
  for (const ln of readFileSync(poolFile, 'utf-8').split('\n').filter(Boolean)) {
    try {
      const r = JSON.parse(ln) as IndexRecord
      if (r.url && (r.ongoing ?? false)) ongoing.push(r)
    } catch { /* 忽略坏行 */ }
  }
  ctx.logger.info('[update] 连载中小说 %d 本（池 %s）', ongoing.length, poolFile)

  // 2) 已抓章节集合
  const outSeen = new Set<string>()
  const outFile = join(process.cwd(), 'out', `${source.id}.jsonl`)
  if (existsSync(outFile)) {
    for (const ln of readFileSync(outFile, 'utf-8').split('\n').filter(Boolean)) {
      try {
        const r = JSON.parse(ln) as { url?: string }
        if (r.url) outSeen.add(r.url)
      } catch { /* 忽略坏行 */ }
    }
  }
  ctx.logger.info('[update] 已抓条目 %d（用于章节 diff）', outSeen.size)

  // 3) 回访详情页 → diff → 派发增量
  const prefix = source.parseRule?.chapterLinkPrefix ?? ''
  const signal = source.parseRule?.chapterSignal ?? ''
  let queue = 0
  let idx = 0
  const worker = async (): Promise<void> => {
    while (idx < ongoing.length) {
      const rec = ongoing[idx++]
      await sleep(delayMs)
      try {
        const r = await ctx.scheduler.fetchFor(source.id, rec.url, 30000)
        if (!r.ok || !r.body) {
          tlog({ ev: 'update_fetch_fail', url: rec.url })
          continue
        }
        const $ = cheerio.load(decodeBytes(r.body, source.parseRule?.encoding))
        if (signal && !r.body.toString().includes(signal)) continue
        const chaps: string[] = []
        $('a[href]').each((_, el) => {
          const href = $(el).attr('href') ?? ''
          if (prefix && !href.includes(prefix)) return
          try {
            const abs = new URL(href, rec.url).href
            if (abs !== rec.url && !outSeen.has(abs)) chaps.push(abs)
          } catch { /* 忽略 */ }
        })
        if (chaps.length) {
          queue += chaps.length
          void ctx.scheduler.push(chaps, source.id, 0)
          ctx.logger.info('[update] %s 新增 %d 章', rec.url, chaps.length)
          tlog({ ev: 'update_new_chapters', url: rec.url, n: chaps.length })
        }
      } catch (e) {
        tlog({ ev: 'update_fetch_err', url: rec.url, err: String(e).slice(0, 100) })
      }
    }
  }
  const workers = Array.from({ length: Math.max(1, concurrency) }, worker)
  await Promise.all(workers)
  ctx.logger.info('[update] 完成回访 %d 本，派发新章节 %d 个', ongoing.length, queue)
  return queue
}