/**
 * 可追溯日志（监控"慢" vs "卡"）：
 * - tlog() 追加 JSONL 到 state/run.log：{ts, ev, ...}
 * - 心跳：调度器每 30s 打一次 hb（队列/在飞/并发/统计）；若在飞请求超过 90s 无完成 → stall_warn
 * - 索引阶段：每页/每分类进度；爬取阶段：请求级起止 + 里程碑
 * 日志写失败不影响主流程（try/catch 包裹）。
 */
import { appendFileSync, mkdirSync, writeFileSync } from 'node:fs'
import { dirname, join } from 'node:path'

let logFile = 'state/run.log'

export function setTraceFile(p: string) {
  logFile = p
}

export function tlog(obj: Record<string, unknown>) {
  try {
    const line = JSON.stringify({ ts: new Date().toISOString(), ...obj }) + '\n'
    mkdirSync(dirname(logFile), { recursive: true })
    appendFileSync(logFile, line, 'utf-8')
  } catch {
    /* 日志失败不阻塞主流程 */
  }
}


/** 初始化一个新的日志文件（每次运行清空，便于从 0 追溯） */
export function freshLogFile() {
  try {
    mkdirSync(dirname(logFile), { recursive: true })
    writeFileSync(logFile, '', 'utf-8')
  } catch {
    /* 忽略 */
  }
}