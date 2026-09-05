/**
 * 入口：加载配置 → (断点恢复) → 挂服务插件 → 依次处理各源（indexer 发现 → scheduler 抓取 → 管线落盘）。
 *
 * 稳定性：SIGINT/SIGTERM 时自动 checkpoint（队列/已访问/失败快照），下次启动续跑；
 * 自检模式：注入假客户端，验证 并发/重试/重入队/去重/失败清单/索引发现(实例化) 全链路（无需网络）。
 *
 * 用法（CLI）：
 *   node --experimental-strip-types src/cli.ts [--config <json>]   # 正式（按源抓取）
 *   node --experimental-strip-types src/cli.ts --self-test         # 自检
 *   node --experimental-strip-types src/cli.ts --cordis app.cordis.yml  # cordis 声明式装配（推荐，见 examples/）
 *
 * 用法（库）：
 *   createApp(cfg, opts)          —— 独立 Context 上装配核心服务（可替换服务/插件/钩子）
 *   assembleApp(ctx, cfg, opts)   —— 在【现有】cordis Context 上装配（fetcher 插件内部用，站点插件同树共享 DI）
 *   runPhase(app, cfg, opts)      —— 两阶段执行（索引发现 → 爬取落盘 → 兜底重试）
 *   fetcherPlugin                 —— cordis 插件形式（src/fetcher.ts），配合 loader.ts 的 cordis.yml 使用
 */
import { Context } from '@deepseek-ai/cordis'
import { appendFileSync, existsSync, mkdirSync, readFileSync, readdirSync, rmSync, statSync, writeFileSync } from 'node:fs'
import { open as openP } from 'node:fs/promises'
import { dirname, join, resolve } from 'node:path'
import { loadConfig, type FetcherConfig, type SchedulerConfig, type SourceConfig, type StorageConfig } from './config.ts'
import { SchedulerService, type SchedulerSnapshot } from './services/scheduler.ts'
export { SchedulerService, IndexerService, ParserService, StorageService }
import { IndexerService } from './services/indexer.ts' // re-export 见下方
import { ParserService } from './services/parser.ts'
import { StorageService } from './services/storage.ts'
import { pipelinePlugin } from './plugins/pipeline.ts'

// 站点插件常用工具与类型（经 'motex-fetcher-core' 直接导入）
export { extractLinks, decodeBytes, extractWithSelectors, detectNextPage } from './rules.ts'
export type { SiteHandler } from './types.ts'

import { runUpdate } from './discovery/update.ts'
import { freshLogFile, setTraceFile, tlog } from './trace.ts'

const enc = new TextEncoder()

/** 源输出目录（绝对）：源级 outDir 优先，缺省回退全局 storage.outDir（2026-09-05 架构：站点扩展自己声明输出位置） */
function sourceOutDir(cfg: FetcherConfig, srcId: string): string {
  const s = cfg.sources.find((x) => x.id === srcId)
  return join(process.cwd(), s?.outDir ?? cfg.storage.outDir ?? 'out')
}

/**
 * 扩展选项：把 cordis 的 DI/事件/插件能力暴露给使用方——
 * - services：替换核心服务（自定义 scheduler/storage/parser/indexer，可继承默认类魔改）
 * - plugins：注册额外 cordis 插件（监听事件、注入服务）
 * - beforeServices / afterServices：装配前后钩子（任意魔改，如覆盖 scheduler.client 自定义网络层）
 */
export interface FetchCoreOptions {
  services?: Partial<{
    scheduler: new (ctx: Context, cfg: SchedulerConfig, sources?: SourceConfig[]) => SchedulerService
    indexer: new (ctx: Context) => IndexerService
    parser: new (ctx: Context) => ParserService
    storage: new (ctx: Context, cfg: StorageConfig, sources?: SourceConfig[]) => StorageService
  }>
  plugins?: any[]
  beforeServices?: (ctx: Context, cfg: FetcherConfig) => void
  afterServices?: (ctx: Context, cfg: FetcherConfig) => void
}

/** 在【给定】cordis 上下文上装配核心服务（可替换服务/注册插件/钩子魔改），返回同一 ctx。
 *  用途：fetcher 插件把服务装到 loader 的 ctx 树上，与站点插件共享 DI/事件（站点经 ctx.get('site.<id>') 被 indexer 分派）。 */
export function assembleApp(ctx: Context, cfg: FetcherConfig, opts: FetchCoreOptions = {}): Context {
  opts.beforeServices?.(ctx, cfg)
  const S = opts.services?.scheduler ?? SchedulerService
  new S(ctx, cfg.scheduler, cfg.sources)
  const I = opts.services?.indexer ?? IndexerService
  new I(ctx)
  const P = opts.services?.parser ?? ParserService
  new P(ctx)
  const St = opts.services?.storage ?? StorageService
  new St(ctx, cfg.storage, cfg.sources)
  ctx.plugin(pipelinePlugin, cfg)
  for (const p of opts.plugins ?? []) ctx.plugin(p, cfg)
  opts.afterServices?.(ctx, cfg)
  return ctx
}

/** 独立 cordis 根上下文上装配核心服务（等价 assembleApp(new Context(), ...)），返回 Context 供使用方自由扩展 */
export function createApp(cfg: FetcherConfig, opts: FetchCoreOptions = {}): Context {
  return assembleApp(new Context(), cfg, opts)
}

/** 自检准备（main 与 fetcher 插件共用）：目录隔离 + 假站点源注入 + 清理上次残留。
 *  ⚠️ 必须在服务装配【之前】调用（血的教训：服务构造时读取 outDir/stateFile）。 */
export function prepareSelfTest(cfg: FetcherConfig) {
  cfg.storage.outDir = 'out-selftest'
  cfg.scheduler.stateFile = 'state-selftest/run.json'
  cfg.indexFile = 'pool-selftest/index.jsonl'
  // 清理上次自检残留（防断言被旧产物污染：pgJoined 要求恰好 1 行、fails 条数精确等）
  for (const dir of ['out-selftest', 'state-selftest', 'pool-selftest']) {
    try { rmSync(join(process.cwd(), dir), { recursive: true, force: true }) } catch { /* 不存在则忽略 */ }
  }
  if (!cfg.sources.length) {
    cfg.sources = [
      { id: 'selftest', kind: 'static', seedUrls: [
        'https://fake.local/a', 'https://fake.local/b', 'https://fake.local/c',
        'https://fake.local/retry-me',       // 前 3 次瞬时失败 → 第 4 次成功（验证重试+重入队）
        'https://fake.local/perm-404',       // 永远 404 → 终局失败进 fails.jsonl
        'https://fake.local/perm-404',       // 重复 URL → 去重跳过
      ] },
      // 站点实例化验证：index 源 = 目录页按 linkRegex 发现 2 个章节页 → 走同一管线
      {
        id: 'selftest-cat', kind: 'index', seedUrls: ['https://fake.local/catalog'],
        indexRule: { linkRegex: 'href="(https://fake\\.local/page\\d+\\.html)"' },
        parseRule: {
          encoding: 'utf-8',
          bodyStart: '<div id="content">', bodyEnd: '</div>',
          adLineRegex: ['www\\.', '广告'],
          minLen: 40,
        },
      },
      // 站点处理器分发 + 标题关键字过滤验证（禁词标题不抓）
      {
        id: 'selftest-site', kind: 'site', siteHandler: 'selftest-site', seedUrls: [],
        filterKeywords: ['禁词'],
        parseRule: { encoding: 'utf-8', section: '#body', contentSelector: '.txt', minLen: 40 },
      },
      // 内容分页跟随验证：首页带“下一页” → 续页拼接成一条
      {
        id: 'selftest-pg', kind: 'static', seedUrls: ['https://fake.local/pg.html'],
        parseRule: {
          encoding: 'utf-8', section: '#wznr', contentSelector: '.ttnr',
          followPagination: true, minLen: 10,
        },
      },
      // 二进制直落盘验证：downloadRaw 源 → 文件 + 元数据（不经解析）
      {
        id: 'selftest-raw', kind: 'static', downloadRaw: true,
        seedUrls: ['https://fake.local/audio/sample.wav'],
      },
    ]
  }
}

/** runPhase 参数（CLI 解析出的 --phase/--limit/--concurrency 与 fetcher 插件配置共用） */
export interface RunPhaseOptions {
  phase?: 'index' | 'crawl' | 'both' | 'update'
  limit?: number
  concurrency?: number
  selfTest?: boolean
}

/** 两阶段执行（CLI main 与 fetcher 插件共用）：索引发现 → 爬取落盘 → 兜底重试 → (自检断言)。
 *  优雅暂停（state/pause_crawls.flag）：checkpoint 落盘后干净返回 { paused: true }，由调用方决定退出方式。 */
export async function runPhase(app: Context, cfg: FetcherConfig, opts: RunPhaseOptions = {}): Promise<{ paused: boolean }> {
  const selfTest = opts.selfTest ?? false

  // ===== 两阶段模式（前置声明：自检/恢复分支也要用） =====
  const indexFile = cfg.indexFile ?? 'pool/index.jsonl'
  const phase = (opts.phase ?? cfg.phase ?? 'both') as 'index' | 'crawl' | 'both' | 'update'
  const limit = opts.limit ?? 0
  if (phase === 'index' || phase === 'both') {
    app.indexer.beginIndex(indexFile)         // 开索引池：处理器逐条 pushIndex 落盘
  }
  // 并行多站时日志按站分文件，避免互踩
  const sid = cfg.sources[0]?.id ?? 'run'
  setTraceFile(join(process.cwd(), 'state', `${sid}.log`))
  freshLogFile()
  tlog({ ev: 'start', phase, sources: cfg.sources.map((s) => s.id), limit })
  const wantConc = opts.concurrency ?? 0
  if (wantConc > 0) app.scheduler.setConcurrency(wantConc)

  if (selfTest) {
    // 自检快速验证重入队（目录隔离已在服务构造前完成）
    cfg.scheduler.requeueDelayMs = 50
    let retryCalls = 0
    app.scheduler.client = async (url: string) => {
      await new Promise((r) => setTimeout(r, 20))
      if (url.includes('retry-me')) {
        retryCalls++
        if (retryCalls < 4) throw new Error('模拟瞬时网络故障')
        return { status: 200, ok: true, body: enc.encode(`<html><body><p>重试后成功：${url}。这是一段足够长的正文填充文本，用来验证解析器最小长度门槛能够放行并成功落盘。</p></body></html>`) }
      }
      if (url.includes('perm-404')) return { status: 404, ok: false, body: null }
      if (url.includes('catalog')) {
        return { status: 200, ok: true, body: enc.encode(
          `<html><head><title>目录</title></head><body><a href="https://fake.local/page1.html">第一章</a>` +
          `<a href="https://fake.local/page2.html">第二章</a><a href="https://fake.local/other.html">无关</a></body></html>`) }
      }
      if (url.includes('page1') || url.includes('page2')) {
        return { status: 200, ok: true, body: enc.encode(
          `<html><body><div id="content"><p>这是章节正文，来自 ${url}。这句是正文主体部分，长度足以通过最小长度门槛，并且应当被完整保留。</p>` +
          `<p>广告 www.bad-ad.com 这一整行应当被行级广告正则过滤掉。</p></div></body></html>`) }
      }
      if (url.includes('fake.local/n')) {
        return { status: 200, ok: true, body: enc.encode(
          `<html><body><div id="body"><div class="nrtitle"><h1>标题${url}</h1></div><div class="txt">这是《${url}》的正文内容，一段足够长以便通过最小长度门槛的文本示例。</div></div></body></html>`) }
      }
      if (url === 'https://fake.local/pg.html' || url === 'https://fake.local/pg_2.html') {
        if (url.endsWith('_2.html')) {
          return { status: 200, ok: true, body: enc.encode(
            `<html><body><div id="wznr"><div class="ttnr"><p>第二页正文内容，需要有足够长度才能通过最小门槛检查并参与拼接。</p></div></div></body></html>`) }
        }
        return { status: 200, ok: true, body: enc.encode(
          `<html><body><div id="wznr"><div class="ttnr"><p>第一页正文内容，需要有足够长度才能通过最小门槛检查并参与拼接。</p></div>` +
          `<a href="https://fake.local/pg_2.html">下一页</a></div></body></html>`) }
      }
      if (url.endsWith('sample.wav')) {
        // 二进制：伪 WAV 头（RIFF....WAVEfmt + 数据）
        return { status: 200, ok: true, body: new Uint8Array([
          0x52, 0x49, 0x46, 0x46, 0x24, 0x00, 0x00, 0x00, 0x57, 0x41, 0x56, 0x45,
          0x66, 0x6d, 0x74, 0x20, 0x10, 0x00, 0x00, 0x00, 0x01, 0x00, 0x01, 0x00,
          0x40, 0x1f, 0x00, 0x00, 0x80, 0x3e, 0x00, 0x00, 0x02, 0x00, 0x10, 0x00,
          0x64, 0x61, 0x74, 0x61, 0x00, 0x00, 0x00, 0x00]) }
      }
      return { status: 200, ok: true, body: enc.encode(`<html><body><p>来自 ${url} 的测试正文。这是一段足够长的正文填充文本，用来验证解析器最小长度门槛能够放行并成功落盘。</p></body></html>`) }
    }
  } else if (phase !== 'index') {
    // 断点恢复（仅爬取阶段）：存在状态文件 → 回填未终局队列（配合 JSONL 输出续跑）
    const statePath = join(process.cwd(), cfg.scheduler.stateFile)
    if (cfg.scheduler.stateFile && existsSync(statePath)) {
      const snap = JSON.parse(readFileSync(statePath, 'utf-8')) as SchedulerSnapshot
      app.scheduler.restore(snap)
      tlog({ ev: 'restored', queue: snap.queue.length, visited: snap.visited.length })
    }
    // 退出钩子：Ctrl+C / kill 时先落 checkpoint
    for (const sig of ['SIGINT', 'SIGTERM'] as const) {
      process.on(sig, async () => {
        await app.scheduler.checkpoint()
        process.exit(0)
      })
    }
  }

  // 管理服务（可选开启，默认关闭）
  if (cfg.manage?.enabled) {
    const { startManageServer } = await import('./services/manage.ts')
    startManageServer(cfg)
  }

  const t0 = Date.now()
  let total = 0
  const pool: { url: string; source: string }[] = []

  // ===== 优雅暂停（crawl_ctl.py 管理）：检测 state/pause_crawls.flag → checkpoint 落盘 → 干净退出 =====
  // 不依赖外部信号（Windows 无 SIGTERM 投递），crawler 自检标记文件，5s 内响应。
  // index 阶段（BFS）同样响应：BFS 幂等 + pool 已落盘，直接退出无损。
  // 注意：先 checkpoint 再置 paused（外部观察到暂停时快照必然已写）；selfTest 不注册（避免定时器挂住进程）。
  const pauseFile = join(process.cwd(), 'state', 'pause_crawls.flag')
  let paused = false
  const pauseIv: ReturnType<typeof setInterval> | null = !selfTest ? setInterval(async () => {
    try {
      if (existsSync(pauseFile)) {
        clearInterval(pauseIv)
        tlog({ ev: 'pause_requested' })
        try {
          await app.scheduler.checkpoint()
        } catch (e) {
          tlog({ ev: 'pause_checkpoint_err', err: String(e).slice(0, 80) })
        }
        paused = true
        app.scheduler.pause()
        app.emit('pause/clean')
        tlog({ ev: 'paused_clean', ok: app.scheduler.stats.ok, failed: app.scheduler.stats.failed, requeued: app.scheduler.stats.requeued })
      }
    } catch { /* 检测失败不阻塞主流程 */ }
  }, 5000).unref() : null

  if (phase !== 'crawl') {
    for (const source of cfg.sources) {
      if (paused) break
      app.logger.info('处理源 %s (kind=%s)：建索引…', source.id, source.kind)
      const urls = await app.indexer.discover(source)
      app.logger.info('源 %s：待抓 %d 个 URL', source.id, urls.length)
      for (const u of urls) pool.push({ url: u, source: source.id })
      total += urls.length
    }
  }

  if (phase !== 'index') {
    // 爬取进度里程碑（每 2500 条成功公告一次；完成时公告总账）
    let lastAnn = 0
    const iv: ReturnType<typeof setInterval> | null = !selfTest
      ? setInterval(() => {
          const st = app.scheduler.stats
          if (st.ok - lastAnn >= 2500) {
            lastAnn = st.ok
            tlog({ ev: 'crawl_progress', ok: st.ok, failed: st.failed, requeued: st.requeued, conc: app.scheduler.config.concurrency, pool: pool.length })
          }
        }, 60_000)
      : null
    const s = app.scheduler.stats
    try {
      if (phase === 'update') {
        // 追更：连载小说增量（回访详情 → diff 新章节 → 增量抓取，不动旧章节）
        const src0 = cfg.sources[0]
        const upConc = opts.concurrency || 2
        const added = await runUpdate(app, src0, indexFile, upConc, 1500)
        app.logger.info('追更完成：派发新章节 %d', added)
        tlog({ ev: 'update_plan', added })
      } else {
        if (phase === 'crawl') {
          // 从索引池恢复爬取任务（按源增量读：源级 indexFile 优先；进程内每轮只读新追加字节）
          for (const src of cfg.sources) {
            const file = sourceIndexFile(cfg, src)
            const n = await readPoolDelta(app, cfg, src, pool)
            tlog({ ev: 'pool_read', source: src.id, file, added: n })
            app.logger.info('索引池 %s：增量 %d 条', file, n)
          }
        }
        await runCrawlTail(app, cfg, pool, { limit, selfTest })
      }
    } finally {
      if (iv) clearInterval(iv)
      tlog({ ev: 'end', phase, ok: s.ok, failed: s.failed, requeued: s.requeued, skipped: s.skipped })
    }
  }

  const s = app.scheduler.stats
  const secs = ((Date.now() - t0) / 1000).toFixed(1)
  if (selfTest) {
    // 自检断言（框架 + 站点实例化验收）
    const lines = readOutputLines(cfg, 'selftest.jsonl')
    const catLines = readOutputLines(cfg, 'selftest-cat.jsonl')
    const siteLines = readOutputLines(cfg, 'selftest-site.jsonl')
    const pgLines = readOutputLines(cfg, 'selftest-pg.jsonl')
    const fails = readOutputLines(cfg, 'selftest.fails.jsonl')
    // 二进制直落盘断言：文件存在 + 元数据一行
    const rawDir = join(process.cwd(), 'out-selftest', 'selftest-raw')
    const rawFiles = existsSync(rawDir) ? readdirSync(rawDir) : []
    const rawMeta = readOutputLines(cfg, 'selftest-raw.meta.jsonl')
    const expect = {
      stats: s.ok === 11 && s.failed === 2 && s.skipped === 1 && s.requeued >= 1,
      retryOk: lines.some((l) => l.includes('retry-me')),
      indexFound: catLines.some((l) => l.includes('page1')) && catLines.some((l) => l.includes('page2')),
      adFiltered: !catLines.some((l) => l.includes('bad-ad')),
      siteFiltered: !siteLines.some((l) => l.includes('fake.local/n2'))
        && siteLines.some((l) => l.includes('fake.local/n1')) && siteLines.some((l) => l.includes('fake.local/n3')),
      pgJoined: pgLines.length === 1 && pgLines[0].includes('第一页正文') && pgLines[0].includes('第二页正文'),
      failListed: fails.length >= 2,          // 终局失败 + 兜底重试轮再失败 = 2 条记录
      rawSaved: rawFiles.some((f) => f.endsWith('.wav')) && rawMeta.length === 1 && rawMeta[0].includes('sample.wav'),
    }
    console.log(`[self-test] ok=${s.ok} failed=${s.failed} skipped=${s.skipped} requeued=${s.requeued} (${secs}s)`)
    console.log('[self-test]', Object.values(expect).every(Boolean) ? 'ALL PASS ✅' : JSON.stringify(expect))
  } else {
    console.log(`[motex-fetcher] 完成：成功 ${s.ok} / 终局失败 ${s.failed} / 重入队 ${s.requeued} / 去重跳过 ${s.skipped}（${secs}s）`)
  }
  return { paused }
}

/** 源索引池文件（相对运行目录）：源级 indexFile 优先；单源回退全局 indexFile；多源缺省 pool/<id>.index.jsonl */
function sourceIndexFile(cfg: FetcherConfig, src: SourceConfig): string {
  if (src.indexFile) return src.indexFile
  if (cfg.sources.length === 1 && cfg.indexFile) return cfg.indexFile
  return `pool/${src.id}.index.jsonl`
}

/** 池文件游标（进程内，2026-09-05 单进程常驻）：每轮只读【新追加字节】——
 *  旧实现每轮全量同步读池（SMR 盘上 48k 行可卡 14s+ 且冻结事件循环，试点实测）。
 *  池文件只追加不重写 → 字节游标安全；文件被截断/重建 → 归零全量重扫。 */
const poolCursors = new WeakMap<FetcherConfig, Map<string, { pos: number; tail: string }>>()

async function readPoolDelta(
  app: Context, cfg: FetcherConfig, src: SourceConfig,
  sink: { url: string; source: string }[],
): Promise<number> {
  const file = sourceIndexFile(cfg, src)
  let cursors = poolCursors.get(cfg)
  if (!cursors) {
    cursors = new Map()
    poolCursors.set(cfg, cursors)
  }
  const p = join(process.cwd(), file)
  let size = 0
  try { size = (await statP(p)).size } catch { return 0 }
  let cur = cursors.get(file) ?? { pos: 0, tail: '' }
  if (size < cur.pos) cur = { pos: 0, tail: '' }        // 截断/重建 → 全量
  if (size === cur.pos) {
    cursors.set(file, cur)
    return 0
  }
  let added = 0
  let fh: Awaited<ReturnType<typeof openP>> | null = null
  try {
    fh = await openP(p, 'r')
    const st = await fh.stat()
    const len = st.size - cur.pos
    const buf = Buffer.alloc(len)
    let read = 0
    while (read < len) {
      const { bytesRead } = await fh.read(buf, read, len - read, cur.pos + read)
      if (bytesRead <= 0) break
      read += bytesRead
    }
    cur.pos += read
    const text = cur.tail + buf.toString('utf8', 0, read)
    const lines = text.split('\n')
    cur.tail = text.endsWith('\n') ? '' : (lines.pop() ?? '')
    for (const ln of lines) {
      if (!ln.trim()) continue
      try {
        const r = JSON.parse(ln) as { url?: string; source?: string }
        if (r.url) {
          sink.push({ url: r.url, source: r.source ?? src.id })
          added++
        }
      } catch { /* 坏行忽略 */ }
    }
  } catch (e) {
    app.logger.warn('[pool] 增量读失败 %s: %s', file, String(e).slice(0, 120))
  } finally {
    if (fh) await fh.close()
  }
  cursors.set(file, cur)
  return added
}

/** 爬取收尾（runPhase 与 watch 常驻扫池共用）：skipExisting → 限数 → 按源入队（force）→ 落定 → 兜底重试。
 *  优雅暂停：waitIdle 立即返回；已暂停则跳过兜底重试（checkpoint 已含失败清单，恢复后再跑）。 */
export async function runCrawlTail(
  app: Context, cfg: FetcherConfig,
  pool: { url: string; source: string }[],
  opts: { limit?: number; selfTest?: boolean } = {},
): Promise<void> {
  const limit = opts.limit ?? 0
  const selfTest = opts.selfTest ?? false
  const s = app.scheduler.stats
  if (cfg.skipExisting) {
    // 跳过已在输出 JSONL 中存在的 url（断点式续爬）。
    // 已爬集合由 storage 维护：进程内惰性全量载入一次 + append 增量（2026-09-05 单进程常驻——
    // 旧实现每轮全量同步读 done.urls，SMR 盘上可卡死事件循环数分钟，试点实测）。
    const doneMap = new Map<string, Set<string>>()
    const srcs = new Set(pool.map((p) => p.source))
    tlog({ ev: 'skip_load', sources: [...srcs] })
    await Promise.all([...srcs].map(async (src) => {
      doneMap.set(src, await app.storage.doneSet(src))
    }))
    const before = pool.length
    const kept = pool.filter((p) => !doneMap.get(p.source)?.has(p.url))
    app.logger.info('skipExisting：过滤 %d 条已爬，剩 %d 条', before - kept.length, kept.length)
    pool.length = 0
    for (const k of kept) pool.push(k)      // 不用 ...spread：十几万元素会被调用栈限制爆掉
  }
  if (limit > 0) pool.length = Math.min(pool.length, limit)
  tlog({ ev: 'crawl_plan', pool: pool.length })
  // 按源分批入队（并发/重试/去重/断点保护由调度器负责）
  const bySrc = new Map<string, string[]>()
  for (const p of pool) {
    if (!bySrc.has(p.source)) bySrc.set(p.source, [])
    bySrc.get(p.source)!.push(p.url)
  }
  // downloadRaw 源（大文件）走流式直落盘，不进内存队列
  // （selfTest 排除：自检用 mock client 验证 saveBinary 小文件路径）
  if (!selfTest) {
    for (const src of cfg.sources) {
      if (!src.downloadRaw) continue
      const urls = bySrc.get(src.id)
      if (!urls || !urls.length) continue
      app.logger.info('downloadRaw 源 %s：流式下载 %d 个文件', src.id, urls.length)
      tlog({ ev: 'raw_download_start', source: src.id, n: urls.length })
      const r = await app.storage.downloadRawFiles(urls, src.id)
      tlog({ ev: 'raw_download_done', source: src.id, ok: r.ok, skipped: r.skipped, failed: r.failed })
      bySrc.delete(src.id)
    }
  }
  // 入队（2026-09-05 多源公平）：按 CHUNK 轮转各源入队（不逐批 await——按批结算已修，
  // 多批可同时在队），避免大池源整块占队头把其余源饿死几小时（试点实测：单源独占 4 并发窗）。
  const CHUNK = 500
  const entries = [...bySrc.entries()]
  const maxLen = Math.max(0, ...entries.map(([, u]) => u.length))
  for (let pos = 0; pos < maxLen; pos += CHUNK) {
    for (const [sid, urls] of entries) {
      const slice = urls.slice(pos, pos + CHUNK)
      if (slice.length) {
        // force：池 URL 绕过 visited（2026-09-05 指纹风控站 崩溃恢复死循环修复——
        // restore 恢复的 visited 含上次会话在跑的池 URL，push 全被去重跳过 → 无事可做秒退；
        // 池 URL 防重由 skipExisting/done.urls 负责，force 安全）
        void app.scheduler.push(slice, sid, 0, { force: true })
      }
    }
  }
  await app.scheduler.waitIdle()     // 全部落定（含分页续推）；暂停后立即返回
  tlog({ ev: 'crawl_main_done', ok: s.ok, failed: s.failed, requeued: s.requeued, skipped: s.skipped })
  // 兜底重试轮：终局失败不能直接放弃（用户要求）；已暂停则跳过（checkpoint 已含失败清单，恢复后再跑）
  if (!app.scheduler.isPaused()) {
    const retryPasses = cfg.scheduler.failRetryPasses ?? 1
    for (let i = 0; i < retryPasses; i++) {
      const n = await app.scheduler.retryFailedPass()
      if (!n) break
      await app.scheduler.waitIdle()
      app.logger.warn('兜底重试第 %d 轮完成（重试 %d 个）', i + 1, n)
      tlog({ ev: 'crawl_retry_done', pass: i + 1, n })
    }
  }
}

/** 常驻扫池一轮（fetcher watch 模式用）：按源加载各自索引池 → 爬取收尾。
 *  等价于标准进程“重启一轮”的效果——watch 进程周期调用即实现无需重启的续爬/追更
 *  （池由独立 index 阶段进程持续追加；池 URL 防重靠 done.urls + force push）。 */
export async function runCrawlSweep(app: Context, cfg: FetcherConfig, opts: RunPhaseOptions = {}): Promise<void> {
  const pool: { url: string; source: string }[] = []
  for (const src of cfg.sources) {
    const file = sourceIndexFile(cfg, src)
    const n = await readPoolDelta(app, cfg, src, pool)
    app.logger.info('[sweep] 池 %s：增量 %d 条', file, n)
  }
  await runCrawlTail(app, cfg, pool, { limit: opts.limit ?? 0, selfTest: opts.selfTest })
}

/** 单源发现轮（fetcher watch 模式用）：开该源索引池 → discover（site 类由 handler pushIndex 增量落池；
 *  非 site 类把返回 URL 直接补池）→ 返回新增条数。旁路抓取（不走调度队列），与正文爬并行。 */
export async function runIndexSweep(app: Context, cfg: FetcherConfig, src: SourceConfig): Promise<number> {
  const file = sourceIndexFile(cfg, src)
  app.indexer.beginIndex(file)                          // 每源轮前切换池文件
  const urls = await app.indexer.discover(src)
  let added = urls.length
  if (src.kind !== 'site' && urls.length) {
    // static/index 源：discover 返回未落池（site 源由 handler pushIndex）→ 手动补
    try {
      const p = join(process.cwd(), file)
      mkdirSync(dirname(p), { recursive: true })
      appendFileSync(p, urls.map((u) => JSON.stringify({ url: u, source: src.id }) + '\n').join(''))
    } catch (e) {
      app.logger.warn('[index] 源 %s 补池失败: %s', src.id, String(e).slice(0, 120))
    }
  }
  app.logger.info('[index] 发现轮 %s：新增 %d 条 → %s', src.id, added, file)
  return added
}

export async function main(argv?: string[], opts: FetchCoreOptions = {}) {
  const args = argv ?? process.argv.slice(2)
  const selfTest = args.includes('--self-test')
  const configPath = parseConfigPath(args)

  const cfg = loadConfig(configPath)
  if (!cfg.sources.length && !selfTest) {
    console.error('[motex-fetcher] 未配置 sources（--config <json> 或补 examples）')
    process.exit(2)
  }
  if (selfTest) prepareSelfTest(cfg)

  // 根上下文：注册全部服务（作用域 DI：服务挂到 ctx 上按名注入）
  // createApp 暴露 cordis 装配能力：服务可替换 / 插件可注册 / 前后钩子可魔改
  const app = createApp(cfg, {
    ...opts,
    beforeServices: (ctx, c) => {
      opts.beforeServices?.(ctx, c)
      if (selfTest) {
        // 假站点处理器：模拟“发现 3 本 → 关键字过滤掉 1 本”（cordis 服务方式注册，经 DI 被 indexer 分派）
        ctx.provide('site.selftest-site', {
          id: 'selftest-site',
          discover: async (_ctx, source) => {
            const kws = source.filterKeywords ?? []
            const found = [
              { url: 'https://fake.local/n1', title: '正常小说标题' },
              { url: 'https://fake.local/n2', title: '含有禁词的小说标题' },
              { url: 'https://fake.local/n3', title: '另一本正常小说' },
            ]
            return found.filter((n) => !kws.some((k) => n.title.includes(k))).map((n) => n.url)
          },
        })
      }
    },
  })

  const result = await runPhase(app, cfg, {
    phase: (getArg(args, '--phase') ?? cfg.phase ?? 'both') as 'index' | 'crawl' | 'both' | 'update',
    limit: Number(getArg(args, '--limit') ?? '0') || 0,
    concurrency: Number(getArg(args, '--concurrency') ?? '0') || 0,
    selfTest,
  })
  // 暂停 = 干净结束（checkpoint 已落盘）：立即退出，不等待残余定时器
  if (result.paused) process.exit(0)
}

function readOutputLines(cfg: FetcherConfig, name: string): string[] {
  try {
    return readFileSync(join(process.cwd(), cfg.storage.outDir, name), 'utf-8').split('\n').filter(Boolean)
  } catch {
    return []
  }
}

function parseConfigPath(args: string[]): string | undefined {
  const i = args.indexOf('--config')
  return i >= 0 && args[i + 1] ? resolve(join(process.cwd(), args[i + 1])) : undefined
}

function getArg(args: string[], name: string): string | undefined {
  const i = args.indexOf(name)
  return i >= 0 && args[i + 1] ? args[i + 1] : undefined
}
