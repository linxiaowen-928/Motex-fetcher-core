/**
 * 落盘服务（已实现）：JSONL 追加写（每行一条 ParsedItem）。
 * 扩展点：去重（url 指纹）、按源分目录、断点续跑等均可在此/监听 fetch/parsed 的插件里做。
 */
import { appendFile, mkdir, open, writeFile } from 'node:fs/promises'
import { createHash } from 'node:crypto'
import {
  appendFileSync, createReadStream, createWriteStream,
  existsSync, readFileSync, readdirSync, statSync,
} from 'node:fs'
import { createInterface } from 'node:readline'
import { spawn } from 'node:child_process'
import { join } from 'node:path'
import type { Context } from '@deepseek-ai/cordis'
import { Service } from '@deepseek-ai/cordis'
import type { SourceConfig, StorageConfig } from '../config.ts'
import type { FetchResponse, ParsedItem } from '../types.ts'

export class StorageService extends Service {
  /** 全局兜底目录（源未声明 outDir 时用） */
  readonly outDir: string
  /** 源 → 输出目录（源级声明；2026-09-05 起站点扩展自己注册输出位置） */
  private readonly sourceDirs = new Map<string, string>()
  private stripNewlines: boolean
  /** 已爬集合（源 → Set<url>；惰性全量载入一次，append 增量维护）——2026-09-05 单进程常驻：
   *  免扫池每轮全量重读 done.urls（SMR 盘同步读可卡死事件循环数分钟，试点实测） */
  private readonly doneSets = new Map<string, Set<string>>()
  private readonly doneLoads = new Map<string, Promise<Set<string>>>()

  constructor(ctx: Context, config: StorageConfig, sources?: SourceConfig[]) {
    super(ctx, 'storage')
    this.outDir = join(process.cwd(), config.outDir ?? 'out')
    for (const s of sources ?? []) {
      if (s.outDir) this.sourceDirs.set(s.id, join(process.cwd(), s.outDir))
    }
    this.stripNewlines = config.stripNewlines ?? true
  }

  /** 条目/源 → 输出目录（源级声明优先，无则全局兜底） */
  private dirFor(source: string): string {
    return this.sourceDirs.get(source) ?? this.outDir
  }

  /** 运行中补充源级输出目录（watch 热接入的源在装配后才注册——2026-09-05） */
  registerSourceDirs(sources: SourceConfig[]): void {
    for (const s of sources ?? []) {
      if (s?.id && s.outDir) this.sourceDirs.set(s.id, join(process.cwd(), s.outDir))
    }
  }

  /** 某源的已爬集合（惰性载入一次；并发调用共享同一载入）。异步流式读——不阻塞事件循环。 */
  async doneSet(source: string): Promise<Set<string>> {
    const p = this.doneLoads.get(source)
    if (p) return p
    const loading = this.loadDoneSet(source)
    this.doneLoads.set(source, loading)
    return loading
  }

  private async loadDoneSet(source: string): Promise<Set<string>> {
    const set = new Set<string>()
    this.doneSets.set(source, set)
    const dir = this.dirFor(source)
    const donePath = join(dir, `${source}.done.urls`)
    if (existsSync(donePath) && statSync(donePath).size > 0) {
      // 快路径：流式读 done.urls（纯 url 行）；载入期间可能又有 append → 尾部补读
      for (let pass = 0; pass < 3; pass++) {
        const size0 = statSync(donePath).size
        const start = pass === 0 ? 0 : size0
        const rl = createInterface({
          input: createReadStream(donePath, { start }),
          crlfDelay: Infinity,
        })
        for await (const ln of rl) {
          const u = ln.trim()
          if (u) set.add(u)
        }
        if (statSync(donePath).size <= size0) break
      }
    } else if (existsSync(dir)) {
      // 慢路径（仅首次，done.urls 缺失）：流式扫 JSONL 全部分片生成 done.urls
      // （避免 GB 级文件全量进内存；生成后后续启动走快路径）
      let files: string[] = []
      try { files = readdirSync(dir) } catch { /* 目录不可读则跳过 */ }
      const jsonls = files.filter((f) => f === `${source}.jsonl` || f.startsWith(`${source}.jsonl.part`)).sort()
      let buf: string[] = []
      for (const jf of jsonls) {
        const rl = createInterface({ input: createReadStream(join(dir, jf)), crlfDelay: Infinity })
        for await (const ln of rl) {
          try {
            const u = (JSON.parse(ln) as { url: string }).url
            if (u) {
              set.add(u)
              buf.push(u)
            }
          } catch { /* 坏行忽略 */ }
          if (buf.length >= 8000) {
            try { appendFileSync(donePath, buf.join('\n') + '\n'); buf = [] } catch { /* 已爬记录失败不致命 */ }
          }
        }
      }
      if (buf.length) {
        try { appendFileSync(donePath, buf.join('\n') + '\n') } catch { /* 同上 */ }
      }
    }
    this.ctx.logger.info('[storage] 已爬集合 %s：%d 条', source, set.size)
    return set
  }

  /** 追加一条正文条目到 <source>.jsonl（默认剥掉文本内换行：训练语料不需要 \n），
   *  并同步追加一行 url 到 <source>.done.urls（已爬记录，重启快速去重用，免全量重扫 JSONL）。 */
  async append(item: ParsedItem): Promise<void> {
    const out = this.sanitize(item)
    const dir = this.dirFor(out.source)
    await this.writeLine(`${out.source}.jsonl`, out, dir)
    try {
      await appendFile(join(dir, `${out.source}.done.urls`), out.url + '\n', 'utf-8')
      this.doneSets.get(out.source)?.add(out.url)    // 内存已爬集合同步增量
    } catch { /* 已爬记录失败不影响正文落盘（最坏下次重爬该 url，幂等） */ }
  }

  private sanitize(item: ParsedItem): ParsedItem {
    if (!this.stripNewlines) return item
    return {
      ...item,
      text: item.text.replace(/\r?\n+/g, ''),
      title: item.title?.replace(/\r?\n+/g, ''),
    }
  }

  /** 追加一条终局失败记录到 <source>.fails.jsonl（配合断点/统计/后续重跑） */
  async appendFail(res: FetchResponse): Promise<void> {
    const dir = this.dirFor(res.source)
    await this.writeLine(`${res.source}.fails.jsonl`, {
      url: res.url, source: res.source, status: res.status,
      error: res.error, retries: res.retries, at: new Date().toISOString(),
    }, dir)
  }

  /** 二进制直落盘（downloadRaw 源：音频/压缩包等）：
   *  文件 → <outDir>/<source>/<sha1(url)前16>.<ext>（幂等命名）；
   *  元数据 → <outDir>/<source>.meta.jsonl（url↔file 映射，校验/后续处理用）。 */
  async saveBinary(res: FetchResponse): Promise<void> {
    if (!res.ok || !res.body) return
    const dir = this.dirFor(res.source)
    await mkdir(join(dir, res.source), { recursive: true })
    let ext = 'bin'
    try {
      const p = new URL(res.url).pathname
      const m = /\.([a-z0-9]{1,6})$/i.exec(p)
      if (m) ext = m[1].toLowerCase()
      const m2 = /\.(tar\.gz|tar\.bz2)$/i.exec(p)
      if (m2) ext = m2[1].toLowerCase()
    } catch { /* 非法 URL 用 bin */ }
    const sha = createHash('sha1').update(res.url).digest('hex').slice(0, 16)
    const file = `${sha}.${ext}`
    const fileDir = join(dir, res.source)
    await writeFile(join(fileDir, file), Buffer.from(res.body))
    await appendFile(join(dir, `${res.source}.meta.jsonl`),
      JSON.stringify({ url: res.url, file, ext, size: res.body.length, at: new Date().toISOString() }) + '\n', 'utf-8')
    this.ctx.logger.debug('saved binary %s/%s (%d B)', res.source, file, res.body.length)
  }

  /** 大文件流式下载（GB 级 tar.gz/音频包）：交给独立 worker 进程执行。
   *  （应用内 undici 连接池会被同源旧连接污染导致请求挂起；独立进程干净环境已验证稳定）
   *  注意：DSH 沙箱禁止 spawn 管道捕获（EPERM）→ stdio 全 ignore，结果写文件。
   *  断点续传：meta.jsonl 有记录 → 跳过。文件 → <outDir>/<source>/<sha1>.<ext> */
  async downloadRawFiles(urls: string[], source: string, _chunks = 8): Promise<{ ok: number; skipped: number; failed: number }> {
    const worker = join(process.cwd(), 'src', 'download_worker.ts')
    const dir = this.dirFor(source)
    const resultPath = join(dir, '.worker_result.json')
    const inputPath = join(dir, '.worker_input.json')
    await mkdir(dir, { recursive: true })
    await writeFile(inputPath, JSON.stringify({ urls, source, outDir: dir, resultPath }), 'utf-8')
    await new Promise<void>((resolve, reject) => {
      // stdio 全 ignore（管道捕获被沙箱 EPERM 拒绝）；输入/结果均经文件
      const cp = spawn(process.execPath, ['--experimental-strip-types', worker, inputPath], { stdio: 'ignore' })
      cp.on('error', reject)
      cp.on('close', (code) => (code === 0 ? resolve() : reject(new Error(`下载 worker 退出码 ${code}`))))
    })
    try {
      const r = JSON.parse(readFileSync(resultPath, 'utf-8')) as { ok: number; skipped: number; failed: number }
      this.ctx.logger.info('downloadRaw 源 %s 完成：成功 %d / 跳过 %d / 失败 %d', source, r.ok, r.skipped, r.failed)
      return r
    } catch {
      return { ok: 0, skipped: 0, failed: urls.length }
    }
  }

  /** 独立 fetch + 单流落盘（不整包进内存；30min 超时） */
  private async streamFetch(url: string, target: string): Promise<void> {
    const ctrl = new AbortController()
    const timer = setTimeout(() => ctrl.abort(), 30 * 60 * 1000)
    try {
      const res = await fetch(url, { signal: ctrl.signal, redirect: 'follow' })
      if (!res.ok || !res.body) throw new Error(`HTTP ${res.status}`)
      await this.streamToFile(res.body, target)
    } finally {
      clearTimeout(timer)
    }
  }

  /** 单流：Web 流 → 文件（不整包进内存） */
  private async streamToFile(stream: ReadableStream<Uint8Array>, target: string): Promise<void> {
    const reader = stream.getReader()
    const ws = createWriteStream(target)
    try {
      for (;;) {
        const { done: d, value } = await reader.read()
        if (d) break
        ws.write(Buffer.from(value))
      }
      await new Promise<void>((resolve, reject) => ws.end((e) => (e ? reject(e) : resolve())))
    } finally {
      reader.releaseLock()
    }
  }

  /** Range 分块并行下载：小块（1MB）× 有限并发（8），写文件对应偏移。
   *  实测：网络层（TUN）对大请求（全量/大 Range）挂起，1MB 以下稳定——块必须小。 */
  private async downloadChunked(url: string, target: string, size: number, _chunks: number): Promise<void> {
    const CHUNK = 1 * 2 ** 20          // 1MB/块（网络层稳定上限）
    const PARALLEL = 8                 // 并发连接数
    const fh = await open(target, 'w')
    let next = 0
    const worker = async () => {
      while (next < size) {
        const start = next
        next = Math.min(size, start + CHUNK)
        await this.downloadRange(url, fh, start, next - 1, Math.floor(start / CHUNK))
      }
    }
    try {
      await Promise.all(Array.from({ length: PARALLEL }, worker))
    } finally {
      await fh.close()
    }
  }

  private async downloadRange(url: string, fh: Awaited<ReturnType<typeof open>>, start: number, end: number, idx: number): Promise<void> {
    for (let attempt = 1; attempt <= 3; attempt++) {
      try {
        const ctrl = new AbortController()
        // 块超时 60s：1MB 块正常 10s 内完成（网络层偶发挂起 → 快速失败重试，不干等）
        const timer = setTimeout(() => ctrl.abort(), 60_000)
        // connection: close —— 禁用连接池复用（应用内同源旧连接可能被污染导致挂起）
        const res = await fetch(url, { headers: { Range: `bytes=${start}-${end}`, connection: 'close' }, signal: ctrl.signal, redirect: 'follow' })
        if (!res.ok || !res.body) {
          clearTimeout(timer)
          throw new Error(`块 ${idx} HTTP ${res.status}`)
        }
        const reader = res.body.getReader()
        let pos = start
        try {
          for (;;) {
            const { done: d, value } = await reader.read()
            if (d) break
            await fh.write(Buffer.from(value), 0, value.length, pos)
            pos += value.length
          }
          clearTimeout(timer)
          return
        } finally {
          reader.releaseLock()
        }
      } catch (e) {
        this.ctx.logger.warn('分块 %d 重试（%d/3）：%s', idx, attempt, String(e).slice(0, 80))
        await new Promise((r) => setTimeout(r, 3000 * attempt))
      }
    }
    throw new Error(`分块 ${idx} 下载失败`)
  }

  /** 分片落盘：单文件超过 SHARD_SIZE(64MB) 自动切下一个分片（<file>.partN），避免单文件过大 */
  private async writeLine(file: string, obj: unknown, dir: string) {
    await mkdir(dir, { recursive: true })
    const shard = this.currentShard(file, dir)
    await appendFile(join(dir, shard), JSON.stringify(obj) + '\n', 'utf-8')
    this.ctx.logger.debug('saved %s', shard)
  }

  /** 分片选择：主文件 <file> 超限 → 找 .part1/.part2/...（各分片也超限则继续递增） */
  private currentShard(file: string, dir: string): string {
    const SHARD = 64 * 2 ** 20
    const p = join(dir, file)
    if (existsSync(p) && statSync(p).size >= SHARD) {
      let n = 1
      for (;;) {
        const sp = join(dir, `${file}.part${n}`)
        if (!existsSync(sp)) return `${file}.part${n}`
        if (statSync(sp).size < SHARD) return `${file}.part${n}`
        n++
      }
    }
    return file
  }
}