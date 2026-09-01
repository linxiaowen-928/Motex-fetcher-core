/**
 * 轻量管理服务（可选开启，默认关闭）：
 * - 纯 API 路线（REST）或 API + 轻量 Web 页面（单文件，无框架）
 * - 能力：任务状态查看（输出/心跳/暂停态）+ 优雅暂停/恢复（pause flag 机制）
 * - 预留：POST /api/instances（图形化新增实例入口，当前返回 501 未实现，接口已定）
 * 零额外依赖：Node 内置 http 模块。
 */
import { createServer, IncomingMessage, ServerResponse } from 'node:http'
import { existsSync, mkdirSync, readFileSync, rmSync, statSync, writeFileSync } from 'node:fs'
import { join } from 'node:path'
import type { FetcherConfig, ManageConfig } from '../config.ts'
import { tlog } from '../trace.ts'

const PAUSE_FLAG = join(process.cwd(), 'state', 'pause_crawls.flag')

function json(res: ServerResponse, code: number, obj: unknown) {
  const body = JSON.stringify(obj)
  res.writeHead(code, { 'Content-Type': 'application/json; charset=utf-8', 'Access-Control-Allow-Origin': '*' })
  res.end(body)
}

function readBody(req: IncomingMessage): Promise<string> {
  return new Promise((resolve) => {
    let s = ''
    req.on('data', (d) => (s += d))
    req.on('end', () => resolve(s))
  })
}

/** 任务状态：out 文件 mtime（运行/停更）、暂停 flag、日志心跳 */
function taskStatus(t: { id: string; outFile?: string; logFile?: string }, paused: boolean) {
  const out = t.outFile ? join(process.cwd(), t.outFile) : null
  const log = t.logFile ? join(process.cwd(), t.logFile) : null
  const now = Date.now()
  const outAge = out && existsSync(out) ? Math.round((now - statSync(out).mtimeMs) / 1000) : null
  const outMB = out && existsSync(out) ? Math.round(statSync(out).size / 2 ** 20 * 10) / 10 : null
  let hb: string | null = null
  if (log && existsSync(log)) {
    try {
      const lines = readFileSync(log, 'utf-8').split('\n').filter(Boolean)
      for (let i = lines.length - 1; i >= 0 && i >= lines.length - 20; i--) {
        try {
          const j = JSON.parse(lines[i])
          if (j.ev === 'hb') { hb = `ok=${j.ok} queue=${j.queue} conc=${j.concurrency}`; break }
        } catch { /* 跳过坏行 */ }
      }
    } catch { /* 忽略 */ }
  }
  return {
    id: t.id,
    paused,
    outAgeSec: outAge,
    outMB,
    heartbeat: hb,
    alive: outAge !== null && outAge < 300,
  }
}

export function startManageServer(cfg: FetcherConfig) {
  const m: ManageConfig = cfg.manage ?? {}
  const port = m.port ?? 8787
  const apiOn = m.api !== false
  const webOn = m.web !== false && apiOn
  // 缺省任务 = 当前 config 的 sources
  const tasks = m.tasks && m.tasks.length
    ? m.tasks
    : cfg.sources.map((s) => ({ id: s.id, outFile: `out/${s.id}.jsonl`, logFile: `state/${s.id}.log` }))
  const webHtml = webOn ? readFileSync(join(process.cwd(), 'src', 'manage', 'web.html'), 'utf-8') : null

  const server = createServer(async (req, res) => {
    const url = (req.url ?? '/').split('?')[0]
    const method = req.method ?? 'GET'
    try {
      if (url === '/' && webOn && webHtml) {
        res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' })
        res.end(webHtml)
        return
      }
      if (url === '/api/status' && method === 'GET' && apiOn) {
        const paused = existsSync(PAUSE_FLAG)
        json(res, 200, { paused, tasks: tasks.map((t) => taskStatus(t, paused)) })
        return
      }
      if (url === '/api/pause' && method === 'POST' && apiOn) {
        mkdirSync(join(process.cwd(), 'state'), { recursive: true })
        if (!existsSync(PAUSE_FLAG)) {
          writeFileSync(PAUSE_FLAG, new Date().toISOString(), 'utf-8')
        }
        tlog({ ev: 'manage_pause' })
        json(res, 200, { ok: true, message: '已写暂停标记（任务将在 5s 内 checkpoint 干净退出）' })
        return
      }
      if (url === '/api/resume' && method === 'POST' && apiOn) {
        if (existsSync(PAUSE_FLAG)) rmSync(PAUSE_FLAG, { force: true })
        tlog({ ev: 'manage_resume' })
        json(res, 200, { ok: true, message: '已恢复（supervisor/看护将自动拉起任务）' })
        return
      }
      // 预留：图形化新增实例入口（后续实现：填占位符生成 config 条目）
      if (url === '/api/instances' && method === 'POST' && apiOn) {
        json(res, 501, { ok: false, message: '接口已预留，未实现（后续：图形化新增 crawl 实例）' })
        return
      }
      json(res, 404, { ok: false, error: 'not found' })
    } catch (e: any) {
      json(res, 500, { ok: false, error: String(e?.message ?? e) })
    }
  })
  server.listen(port, () => {
    tlog({ ev: 'manage_start', port, api: apiOn, web: webOn, tasks: tasks.map((t) => t.id) })
  })
  return server
}