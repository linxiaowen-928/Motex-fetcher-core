/**
 * 能力无损迁移专项验证（capability check）：
 *   1. 分片落盘：单文件 ≥64MB 自动滚切 .part1/.part2/...（core 新增能力，自检数据量小从未触发）
 *   2. 断点恢复 roundtrip：checkpoint 落盘 → 新调度器 restore → visited/failed/stats 一致性 + 去重恢复
 *   3. 兜底重试轮：retryFailedPass 对终局失败按源强制重试（绕过去重）
 *
 * 运行：npm run capability-check
 */
import { Context } from '@deepseek-ai/cordis'
import { existsSync, mkdirSync, readFileSync, rmSync, statSync, writeFileSync } from 'node:fs'
import { join } from 'node:path'
import { SchedulerService } from '../src/services/scheduler.ts'
import { StorageService } from '../src/services/storage.ts'

const CWD = process.cwd()
const SHARD_DIR = join(CWD, '.cap-check', 'out-shard')
const CP_FILE = join(CWD, '.cap-check', 'state', 'run.json')
rmSync(join(CWD, '.cap-check'), { recursive: true, force: true })
mkdirSync(join(CWD, '.cap-check'), { recursive: true })

const enc = new TextEncoder()
const ok = (s: string) => ({ status: 200, ok: true, body: enc.encode(s) })

const results: Record<string, boolean> = {}
const check = (name: string, cond: boolean, detail = '') => {
  results[name] = cond
  console.log(`  ${cond ? '✅' : '❌'} ${name}${detail ? ' — ' + detail : ''}`)
}

// ============ 1. 分片落盘 ============
console.log('[1] 分片落盘（64MB 自动滚切）')
{
  const app = new Context()
  const storage = new StorageService(app, { outDir: join('.cap-check', 'out-shard') })
  const pad = 'x'.repeat(1024) // 1KB 行
  // 先把主文件填到 64MB 边界以上（避免 64000 次逐行 append 的 IO 开销）
  const main = join(SHARD_DIR, 'cap.jsonl')
  mkdirSync(SHARD_DIR, { recursive: true })
  const big = Buffer.alloc(64 * 2 ** 20 + 1024, 'a')
  writeFileSync(main, big)
  // 追加一条 → 应滚到 .part1
  await storage.append({ url: 'https://x/1', source: 'cap', text: 'line-after-roll ' + pad })
  const p1 = join(SHARD_DIR, 'cap.jsonl.part1')
  check('主文件 64MB 后追加滚到 .part1', existsSync(p1) && readFileSync(p1, 'utf-8').includes('line-after-roll'),
    `main=${(statSync(main).size / 2 ** 20).toFixed(0)}MB part1=${existsSync(p1) ? (statSync(p1).size / 2 ** 20).toFixed(3) + 'MB' : '缺失'}`)
  // part1 未满 → 继续写 part1
  await storage.append({ url: 'https://x/2', source: 'cap', text: 'second-in-part1 ' + pad })
  check('part1 未满继续写 part1', readFileSync(p1, 'utf-8').includes('second-in-part1'))
  // part1 填满 → 滚到 .part2
  writeFileSync(p1, Buffer.alloc(64 * 2 ** 20 + 1024, 'b'))
  await storage.append({ url: 'https://x/3', source: 'cap', text: 'line-after-part2 ' + pad })
  const p2 = join(SHARD_DIR, 'cap.jsonl.part2')
  check('part1 满后滚到 .part2', existsSync(p2) && readFileSync(p2, 'utf-8').includes('line-after-part2'),
    existsSync(p2) ? `part2=${(statSync(p2).size / 2 ** 20).toFixed(3)}MB` : '缺失')
  // done.urls 正常维护（分片不影响）
  check('done.urls 照常维护', readFileSync(join(SHARD_DIR, 'cap.done.urls'), 'utf-8').split('\n').filter(Boolean).length === 3)
}

// ============ 2. 断点恢复 roundtrip ============
console.log('[2] 断点恢复 roundtrip（checkpoint → restore）')
{
  const mkScheduler = (stateFile: string, client: (u: string, t: number) => Promise<{ status: number; ok: boolean; body: Uint8Array | null }>) => {
    const app = new Context()
    const s = new SchedulerService(app, {
      concurrency: 2, retries: 0, retryDelayMs: 10, requeueDelayMs: 10, maxAttempts: 2,
      dedupe: true, stateFile, delayMs: 0, timeoutMs: 3000,
    })
    s.client = client
    return { app, s }
  }
  const mock = async (u: string) => (u.includes('bad') ? { status: 404, ok: false, body: null } : ok(`<p>${u} ok</p>`))
  const { s: s1 } = mkScheduler(join('.cap-check', 'state', 'run.json'), mock)
  // u1/u3 成功、u2 永久失败（404）
  await s1.push(['https://c/u1', 'https://c/bad2', 'https://c/u3'], 'cap', 0)
  await s1.checkpoint()
  const snap = JSON.parse(readFileSync(join(CWD, '.cap-check', 'state', 'run.json'), 'utf-8')) as {
    visited: string[]; failed: { url: string }[]; stats: { ok: number; failed: number; skipped: number }
  }
  check('checkpoint 落盘 visited/failed/stats', snap.visited.length === 3 && snap.failed.length === 1 && snap.stats.ok === 2,
    `visited=${snap.visited.length} failed=${snap.failed.length} ok=${snap.stats.ok}`)

  // 新进程语义：restore 后 visited/failed 恢复
  const { s: s2 } = mkScheduler(join('.cap-check', 'state', 'run.json'), mock)
  s2.restore({
    queue: snap.visited.map((url) => ({ url, source: 'cap', depth: 0, createdAt: 0, attempts: 1, requeues: 0 })) as any,
    visited: snap.visited, failed: snap.failed.map((f) => ({ url: f.url, source: 'cap' })), stats: snap.stats,
  })
  check('restore 恢复 stats', s2.stats.ok === 2 && s2.stats.failed === 1)
  // 已抓 URL 再 push → 去重跳过（restore 的 visited 生效）
  await s2.push(['https://c/u1', 'https://c/u3'], 'cap', 0)
  check('恢复后已抓 URL 去重生效（skipped=2）', s2.stats.skipped === 2, `skipped=${s2.stats.skipped}`)
  // 兜底重试轮：终局失败的 bad2 强制重试（force 绕过去重）
  const n = await s2.retryFailedPass()
  await s2.waitIdle()
  check('兜底重试轮：失败任务强制重试', n === 1 && s2.stats.failed === 2,
    `重试 ${n} 个，失败计数 ${s2.stats.failed}（bad2 再次 404 → 终局 +1）`)
}

// ============ 汇总 ============
const allPass = Object.values(results).every(Boolean)
console.log(`\n[capability-check] ${allPass ? 'ALL PASS ✅' : 'FAIL ❌ ' + JSON.stringify(results)}`)
rmSync(join(CWD, '.cap-check'), { recursive: true, force: true })
process.exit(allPass ? 0 : 1)
