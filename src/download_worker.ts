/**
 * 大文件下载独立 worker：由 storage.downloadRawFiles 用 child_process 拉起，
 * 独立进程 = 干净 undici 环境（应用内连接池污染会导致请求挂起，独立进程已验证稳定）。
 * 支持 HF 门控（Authorization Bearer token，从输入 JSON 的 hfToken 字段或环境变量 HF_TOKEN 读）。
 * 协议：argv[2] = 输入 JSON 文件 { urls, source, outDir, resultPath?, hfToken? }
 *       结果写 resultPath（无则 stdout）。
 */
import { createHash } from 'node:crypto'
import { createWriteStream, existsSync, readFileSync } from 'node:fs'
import { open, mkdir, appendFile, writeFile } from 'node:fs/promises'
import { join } from 'node:path'

const CHUNK = 1 * 2 ** 20        // 1MB/块（网络层稳定上限）
const PARALLEL = 4               // 并发连接（慢网下 8 并发每块带宽太低易超时，4 更稳）

let HF_TOKEN = process.env.HF_TOKEN ?? ''
function authHeaders(extra: Record<string, string> = {}): Record<string, string> {
  return HF_TOKEN ? { ...extra, Authorization: `Bearer ${HF_TOKEN}` } : extra
}

async function streamFetch(url: string, target: string): Promise<void> {
  const ctrl = new AbortController()
  const timer = setTimeout(() => ctrl.abort(), 30 * 60 * 1000)
  try {
    const res = await fetch(url, { headers: authHeaders(), signal: ctrl.signal, redirect: 'follow' })
    if (!res.ok || !res.body) throw new Error(`HTTP ${res.status}`)
    const reader = res.body.getReader()
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
  } finally {
    clearTimeout(timer)
  }
}

async function downloadRange(url: string, fh: Awaited<ReturnType<typeof open>>, start: number, end: number, idx: number): Promise<void> {
  for (let attempt = 1; attempt <= 3; attempt++) {
    try {
      const ctrl = new AbortController()
      // 块超时 180s：慢网（<0.1MB/s）下 1MB 块也要 10s+，60s 太紧（8 并发更甚）
      const timer = setTimeout(() => ctrl.abort(), 180_000)
      // connection: close —— 禁用 keep-alive 复用（连接池污染会挂起）
      const res = await fetch(url, { headers: authHeaders({ Range: `bytes=${start}-${end}`, connection: 'close' }), signal: ctrl.signal, redirect: 'follow' })
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
      console.error(`[worker] 分块 ${idx} 重试（${attempt}/3）：${String(e).slice(0, 80)}`)
      await new Promise((r) => setTimeout(r, 3000 * attempt))
    }
  }
  throw new Error(`分块 ${idx} 下载失败`)
}

async function downloadChunked(url: string, target: string, size: number): Promise<void> {
  const fh = await open(target, 'w')
  let next = 0
  const worker = async () => {
    while (next < size) {
      const start = next
      next = Math.min(size, start + CHUNK)
      await downloadRange(url, fh, start, next - 1, Math.floor(start / CHUNK))
    }
  }
  try {
    await Promise.all(Array.from({ length: PARALLEL }, worker))
  } finally {
    await fh.close()
  }
}

function extOf(url: string): string {
  try {
    const p = new URL(url).pathname
    const m2 = /\.(tar\.gz|tar\.bz2|tgz)$/i.exec(p)
    if (m2) return m2[1].toLowerCase()
    const m = /\.([a-z0-9]{1,6})$/i.exec(p)
    if (m) return m[1].toLowerCase()
  } catch { /* bin */ }
  return 'bin'
}

async function main() {
  // 输入与结果都走文件（父进程 stdio 全 ignore：DSH 沙箱禁止管道捕获）
  const inputPath = process.argv[2]
  if (!inputPath) {
    console.error('[worker] 缺少输入文件参数')
    process.exit(1)
  }
  const raw = await (await import('node:fs/promises')).readFile(inputPath, 'utf-8')
  const input = JSON.parse(raw.replace(/^\uFEFF/, '')) as { urls: string[]; source: string; outDir: string; resultPath?: string; hfToken?: string }
  if (input.hfToken) HF_TOKEN = input.hfToken
  const { urls, source, outDir } = input
  await mkdir(join(outDir, source), { recursive: true })
  const metaPath = join(outDir, `${source}.meta.jsonl`)
  const done = new Set<string>()
  if (existsSync(metaPath)) {
    for (const ln of readFileSync(metaPath, 'utf-8').split('\n').filter(Boolean)) {
      try { done.add((JSON.parse(ln) as { url: string }).url) } catch { /* 坏行忽略 */ }
    }
  }
  let ok = 0, skipped = 0, failed = 0
  for (const url of urls) {
    if (done.has(url)) { skipped++; continue }
    const file = `${createHash('sha1').update(url).digest('hex').slice(0, 16)}.${extOf(url)}`
    const target = join(outDir, source, file)
    let size = -1
    let got = false
    for (let attempt = 1; attempt <= 3; attempt++) {
      try {
        const pc = new AbortController()
        const pt = setTimeout(() => pc.abort(), 30_000)
        let probe: Response | null = null
        try {
          probe = await fetch(url, { headers: authHeaders({ Range: 'bytes=0-1023' }), signal: pc.signal, redirect: 'follow' })
        } finally {
          clearTimeout(pt)
        }
        const ranges = probe && probe.status === 206
        const cr = probe?.headers.get('content-range') ?? ''
        const len = cr ? Number(cr.split('/')[1] ?? '-1') : (probe ? Number(probe.headers.get('content-length') ?? '-1') : -1)
        await probe?.body?.cancel()
        if (ranges && len > 10 * 2 ** 20) {
          await downloadChunked(url, target, len)
        } else {
          await streamFetch(url, target)
        }
        size = len
        got = true
        break
      } catch (e) {
        console.error(`[worker] ${url} 下载中断（${attempt}/3）：${String(e).slice(0, 100)}`)
        await new Promise((r) => setTimeout(r, 3000 * attempt))
      }
    }
    if (!got) { failed++; continue }
    await appendFile(metaPath, JSON.stringify({ url, file, ext: extOf(url), size, at: new Date().toISOString() }) + '\n', 'utf-8')
    ok++
    console.log(`[worker] 完成 ${source}/${file}（${Math.round(size / 2 ** 20)}MB）`)
  }
  const result = { ok, skipped, failed }
  if (input.resultPath) {
    await (await import('node:fs/promises')).writeFile(input.resultPath, JSON.stringify(result), 'utf-8')
  } else {
    process.stdout.write(JSON.stringify(result))
  }
}

main().catch((e) => {
  console.error('[worker] 致命:', e)
  process.exit(1)
})
