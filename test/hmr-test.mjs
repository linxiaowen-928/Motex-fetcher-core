/**
 * HMR 集成自检：运行中的抓取进程，往 cordis.yml 热新增一个站点条目 → 自动应用 → 自动抓取，全程不重启。
 *
 * 流程：
 *   1. 临时目录 .hmr-test/ 生成 app.cordis.yml（fetcher watch 常驻 + mock 网络层）+ 空 sources 配置
 *   2. 子进程启动 cli.ts --cordis app.cordis.yml --watch
 *   3. 运行中往 app.cordis.yml 追加 site-a 条目（插件 + config.source）
 *   4. watcher 检测 → include.refresh → 站点插件 apply（provide site.site-a + emit source/register）
 *      → fetcher 单源一轮（discover → push → waitIdle）
 *   5. 断言 out/site-a.jsonl 出现且 ≥2 条（fake.local/a、fake.local/b）
 *
 * 运行：npm run self-test-hmr
 */
import { spawn } from 'node:child_process'
import { existsSync, mkdirSync, rmSync, writeFileSync, appendFileSync, readFileSync, openSync } from 'node:fs'
import { join } from 'node:path'

const ROOT = process.cwd()
const dir = join(ROOT, '.hmr-test')
const sleep = (ms) => new Promise((r) => setTimeout(r, ms))

rmSync(dir, { recursive: true, force: true })
mkdirSync(join(dir, 'sites'), { recursive: true })

writeFileSync(join(dir, 'config.json'), JSON.stringify({ sources: [] }))

writeFileSync(join(dir, 'app.cordis.yml'), `# HMR 自检（运行中热新增站点）
- id: fetcher
  name: './fetcher-mock.ts'
  config:
    config: './config.json'
    watch: true
`)

writeFileSync(join(dir, 'fetcher-mock.ts'), `
import { fetcherPlugin } from '../src/fetcher.ts'
const enc = new TextEncoder()
// mock 网络层：fake.local 一律返回固定 HTML（无需 DNS/外网）
const mockClient = async (url) => {
  await new Promise((r) => setTimeout(r, 10))
  return { status: 200, ok: true, body: enc.encode(
    '<html><body><div id="content"><p>hmr body for ' + url + '，一段足够长的正文用于通过最小长度门槛。</p></div></body></html>') }
}
export default {
  name: 'hmr-fetcher',
  apply: async (ctx, config) => {
    config = { ...config, core: { ...(config.core ?? {}), afterServices: (c) => { c.scheduler.client = mockClient } } }
    return fetcherPlugin.apply(ctx, config)
  },
}
`)

writeFileSync(join(dir, 'sites', 'site-a.ts'), `
import { Context } from '@deepseek-ai/cordis'
export default function siteA(ctx: Context, config: any) {
  ctx.provide('site.site-a', {
    id: 'site-a',
    discover: async () => ['https://fake.local/a', 'https://fake.local/b'],
  })
  // 源配置随事件注册：fetcher watch 模式收到后自动抓取
  if (config?.source) ctx.emit('source/register', config.source)
}
`)

const logFd = openSync(join(dir, 'child.log'), 'w')
const child = spawn(
  process.execPath,
  ['--experimental-strip-types', join(ROOT, 'src', 'cli.ts'), '--cordis', 'app.cordis.yml', '--watch'],
  { cwd: dir, stdio: ['ignore', logFd, logFd] },
)

let failed = false
child.on('error', (e) => console.log('[hmr-test] child ERROR:', e.message))
child.on('exit', (code, sig) => console.log('[hmr-test] child EXIT code=', code, 'sig=', sig))
try {
  await sleep(4500) // 等初始装载 + fetcher 常驻

  // 热新增站点条目（带 source 配置）→ watcher → include.refresh → 站点插件 apply → source/register → fetcher 单源一轮
  appendFileSync(join(dir, 'app.cordis.yml'), `
- id: site-a
  name: './sites/site-a.ts'
  config:
    source:
      id: 'site-a'
      kind: 'static'
      seedUrls: ['https://fake.local/a', 'https://fake.local/b']
      parseRule: { encoding: 'utf-8', section: '#content', contentSelector: 'p', minLen: 10 }
`)

  await sleep(7000) // watcher(300ms 防抖) + 热应用 + 一轮抓取

  const outFile = join(dir, 'out', 'site-a.jsonl')
  const lines = existsSync(outFile) ? readFileSync(outFile, 'utf-8').split('\n').filter(Boolean) : []
  const ok = lines.length >= 2 && lines.some((l) => l.includes('fake.local/a')) && lines.some((l) => l.includes('fake.local/b'))
  console.log(`[hmr-test] ${ok ? 'PASS ✅' : 'FAIL ❌'} 热新增站点自动抓取（${lines.length} 条 → out/site-a.jsonl）`)
  failed = !ok
} finally {
  if (failed) { console.log('[hmr-test] 保留临时目录 .hmr-test/ 供排查（child.log / out/）') }
  child.kill()
  await Promise.race([new Promise((r) => child.once('exit', r)), sleep(1500)])
  try { rmSync(dir, { recursive: true, force: true }) } catch {
    await sleep(500)
    try { rmSync(dir, { recursive: true, force: true }) } catch { /* 忽略 */ }
  }
}
process.exit(failed ? 1 : 0)
