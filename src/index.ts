/**
 * 入口：加载配置 → (断点恢复) → 挂服务插件 → 依次处理各源（indexer 发现 → scheduler 抓取 → 管线落盘）。
 *
 * 稳定性：SIGINT/SIGTERM 时自动 checkpoint（队列/已访问/失败快照），下次启动续跑；
 * 自检模式：注入假客户端，验证 并发/重试/重入队/去重/失败清单/索引发现(实例化) 全链路（无需网络）。
 *
 * 用法：
 *   node --experimental-strip-types src/index.ts [--config <json>]   # 正式（按源抓取）
 *   node --experimental-strip-types src/index.ts --self-test         # 自检
 */
import { Context } from '@deepseek-ai/cordis'
import { appendFileSync, existsSync, mkdirSync, readFileSync, readdirSync, writeFileSync } from 'node:fs'
import { createReadStream } from 'node:fs'
import { createInterface } from 'node:readline'
import { join, resolve } from 'node:path'
import { loadConfig, type FetcherConfig, type SchedulerConfig, type StorageConfig } from './config.ts'
import { SchedulerService, type SchedulerSnapshot } from './services/scheduler.ts'
export { SchedulerService, IndexerService, ParserService, StorageService }
import { IndexerService } from './services/indexer.ts' // re-export 见下方
import { ParserService } from './services/parser.ts'
import { StorageService } from './services/storage.ts'
import { pipelinePlugin } from './plugins/pipeline.ts'
import { registerSiteHandler } from './discovery/registry.ts'
import { runUpdate } from './discovery/update.ts'
import { freshLogFile, setTraceFile, tlog } from './trace.ts'

const enc = new TextEncoder()

/**
 * 扩展选项：把 cordis 的 DI/事件/插件能力暴露给使用方——
 * - services：替换核心服务（自定义 scheduler/storage/parser/indexer，可继承默认类魔改）
 * - plugins：注册额外 cordis 插件（监听事件、注入服务）
 * - beforeServices / afterServices：装配前后钩子（任意魔改，如覆盖 scheduler.client 自定义网络层）
 */
export interface FetchCoreOptions {
  services?: Partial<{
    scheduler: new (ctx: Context, cfg: SchedulerConfig) => SchedulerService
    indexer: new (ctx: Context) => IndexerService
    parser: new (ctx: Context) => ParserService
    storage: new (ctx: Context, cfg: StorageConfig) => StorageService
  }>
  plugins?: any[]
  beforeServices?: (ctx: Context, cfg: FetcherConfig) => void
  afterServices?: (ctx: Context, cfg: FetcherConfig) => void
}

/** 装配核心上下文（可替换服务/注册插件/钩子魔改），返回 cordis Context 供使用方自由扩展 */
export function createApp(cfg: FetcherConfig, opts: FetchCoreOptions = {}): Context {
  const app = new Context()
  opts.beforeServices?.(app, cfg)
  const S = opts.services?.scheduler ?? SchedulerService
  new S(app, cfg.scheduler)
  const I = opts.services?.indexer ?? IndexerService
  new I(app)
  const P = opts.services?.parser ?? ParserService
  new P(app)
  const St = opts.services?.storage ?? StorageService
  new St(app, cfg.storage)
  app.plugin(pipelinePlugin, cfg)
  for (const p of opts.plugins ?? []) app.plugin(p, cfg)
  opts.afterServices?.(app, cfg)
  return app
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
  if (selfTest) {
    // ⚠️ 自检必须与生产数据彻底隔离：独立输出/断点目录（必须在服务构造【之前】改配置，血的教训）
    cfg.storage.outDir = 'out-selftest'
    cfg.scheduler.stateFile = 'state-selftest/run.json'
    cfg.indexFile = 'pool-selftest/index.jsonl'
  }

  if (selfTest && !cfg.sources.length) {
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

  if (selfTest) {
    // 假站点处理器：模拟“发现 3 本 → 关键字过滤掉 1 本”
    registerSiteHandler({
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

  // 根上下文：注册全部服务（作用域 DI：服务挂到 ctx 上按名注入）
  // createApp 暴露 cordis 装配能力：服务可替换 / 插件可注册 / 前后钩子可魔改
  const app = createApp(cfg, opts)

  // ===== 两阶段模式（前置声明：自检/恢复分支也要用） =====
  const indexFile = cfg.indexFile ?? 'pool/index.jsonl'
  const argPhase = getArg(args, '--phase')
  const phase = (argPhase ?? cfg.phase ?? 'both') as 'index' | 'crawl' | 'both' | 'update'
  const limit = Number(getArg(args, '--limit') ?? '0') || 0
  if (phase === 'index' || phase === 'both') {
    app.indexer.beginIndex(indexFile)         // 开索引池：处理器逐条 pushIndex 落盘
  }
  // 并行多站时日志按站分文件，避免互踩
  const sid = cfg.sources[0]?.id ?? 'run'
  setTraceFile(join(process.cwd(), 'state', `${sid}.log`))
  freshLogFile()
  tlog({ ev: 'start', phase, sources: cfg.sources.map((s) => s.id), limit })
  const wantConc = Number(getArg(args, '--concurrency') ?? '0') || 0
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
  const pauseFile = join(process.cwd(), 'state', 'pause_crawls.flag')
  // 注意：selfTest 模式不注册（否则完成后定时器挂着进程不退出，自检会"假卡死"）
  const pauseIv = !selfTest ? setInterval(async () => {
    try {
      if (existsSync(pauseFile)) {
        clearInterval(pauseIv)
        tlog({ ev: 'pause_requested' })
        try {
          await app.scheduler.checkpoint()
        } catch (e) {
          tlog({ ev: 'pause_checkpoint_err', err: String(e).slice(0, 80) })
        }
        tlog({ ev: 'paused_clean', ok: app.scheduler.stats.ok, failed: app.scheduler.stats.failed, requeued: app.scheduler.stats.requeued })
        process.exit(0)
      }
    } catch { /* 检测失败不阻塞主流程 */ }
  }, 5000).unref() : null

  if (phase !== 'crawl') {
    for (const source of cfg.sources) {
      app.logger.info('处理源 %s (kind=%s)：建索引…', source.id, source.kind)
      const urls = await app.indexer.discover(source)
      app.logger.info('源 %s：待抓 %d 个 URL', source.id, urls.length)
      for (const u of urls) pool.push({ url: u, source: source.id })
      total += urls.length
    }
  }

  if (phase !== 'index') {
    // 爬取进度里程碑 → 聊天室（每 2500 条成功公告一次；完成时公告总账）
    let lastAnn = 0
    const iv = !selfTest
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
        const upConc = Number(getArg(args, '--concurrency') || 0) || 2
        const added = await runUpdate(app, src0, indexFile, upConc, 1500)
        app.logger.info('追更完成：派发新章节 %d', added)
        tlog({ ev: 'update_plan', added })
      } else {
        if (phase === 'crawl') {
          // 从索引池恢复爬取任务
          const recs = loadIndex(indexFile)
          for (const r of recs) pool.push({ url: r.url, source: r.source })
          app.logger.info('索引池：%d 条', recs.length)
        }
        if (cfg.skipExisting) {
          // 跳过已在输出 JSONL 中存在的 url（断点式续爬）
          // 快路径：读 <src>.done.urls 已爬记录（storage.append 增量维护，纯 url 行）；
          // 慢路径（仅首次）：流式扫 JSONL 生成 done.urls（避免 GB 级文件全量进内存）。
          const seen = new Set<string>()
          for (const src of new Set(pool.map((p) => p.source))) {
            const donePath = join(process.cwd(), cfg.storage.outDir, `${src}.done.urls`)
            if (existsSync(donePath)) {
              const text = readFileSync(donePath, 'utf-8')
              for (const ln of text.split('\n')) {
                const u = ln.trim()
                if (u) seen.add(u)
              }
              app.logger.info('skipExisting：快路径 %s（%d 条已爬）', donePath, seen.size)
            } else {
              // 慢路径（仅首次）：流式扫 JSONL 全部分片（主文件 + .partN）生成 done.urls
              const outDir = join(process.cwd(), cfg.storage.outDir)
              const jsonls = existsSync(outDir)
                ? readdirSync(outDir).filter((f) => f === `${src}.jsonl` || f.startsWith(`${src}.jsonl.part`)).sort()
                : []
              let n = 0
              let buf: string[] = []
              for (const jf of jsonls) {
                const rl = createInterface({ input: createReadStream(join(outDir, jf)), crlfDelay: Infinity })
                for await (const ln of rl) {
                  try {
                    const u = (JSON.parse(ln) as { url: string }).url
                    if (u) {
                      seen.add(u)
                      buf.push(u)
                    }
                  } catch { /* 坏行忽略 */ }
                  if (buf.length >= 8000) {
                    appendFileSync(donePath, buf.join('\n') + '\n')
                    buf = []
                  }
                }
              }
              if (buf.length) appendFileSync(donePath, buf.join('\n') + '\n')
              n = seen.size
              app.logger.info('skipExisting：首次生成 %s（%d 条已爬，%d 分片流式扫描）', donePath, n, jsonls.length)
            }
          }
          const before = pool.length
          const kept = pool.filter((p) => !seen.has(p.url))
          app.logger.info('skipExisting：过滤 %d 条已爬，剩 %d 条', before - kept.length, kept.length)
          pool.length = 0
          for (const k of kept) pool.push(k)      // 不用 ...spread：十几万元素会被调用栈限制爆掉
        }
        if (limit > 0) pool.length = Math.min(pool.length, limit)
        tlog({ ev: 'crawl_plan', pool: pool.length, phase })
        total = pool.length
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
        for (const [sid, urls] of bySrc) {
          await app.scheduler.push(urls, sid, 0)
        }
      }
      await app.scheduler.waitIdle()     // 全部落定（含分页续推）
      tlog({ ev: 'crawl_main_done', ok: s.ok, failed: s.failed, requeued: s.requeued, skipped: s.skipped })
      // 兜底重试轮：终局失败不能直接放弃（用户要求）
      const retryPasses = cfg.scheduler.failRetryPasses ?? 1
      for (let i = 0; i < retryPasses; i++) {
        const n = await app.scheduler.retryFailedPass()
        if (!n) break
        await app.scheduler.waitIdle()
        app.logger.warn('兜底重试第 %d 轮完成（重试 %d 个）', i + 1, n)
        tlog({ ev: 'crawl_retry_done', pass: i + 1, n })
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

function loadIndex(indexFile: string): { url: string; source: string }[] {
  try {
    const text = readFileSync(join(process.cwd(), indexFile), 'utf-8')
    const out: { url: string; source: string }[] = []
    for (const ln of text.split('\n').filter(Boolean)) {
      try {
        const r = JSON.parse(ln) as { url?: string; source?: string }
        if (r.url) out.push({ url: r.url, source: r.source ?? 'unknown' })
      } catch { /* 忽略坏行 */ }
    }
    return out
  } catch {
    return []
  }
}
