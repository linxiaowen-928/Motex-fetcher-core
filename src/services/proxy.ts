/**
 * 代理池轮换器（支持 http/https 池 + socks4/5 池合并）：
 * - loadPool(paths)：reads pool.json / pool_socks.json（{proxy, proto}）
 * - 条目规范化：http/https 保原样；socks 补 scheme（socks5://ip:port）
 * - next()：round-robin / random；故障代理冷却（reportBad）
 * 高并发多出口正是为了降低单 IP 被目标站屏蔽的风险（尤其按 IP 限流的站）。
 */
import { existsSync, readFileSync } from 'node:fs'

export interface PoolEntry {
  proxy: string
  ms?: number
  ms_target?: number
  proto?: string
}

export class ProxyRotator {
  private pool: PoolEntry[] = []
  private idx = 0
  private cooldown = new Map<string, number>()
  readonly mode: 'round-robin' | 'random'

  constructor(poolPaths: string[], mode: 'round-robin' | 'random' = 'round-robin') {
    this.mode = mode
    const merged: PoolEntry[] = []
    for (const path of poolPaths) {
      if (!path || !existsSync(path)) continue
      try {
        const raw = JSON.parse(readFileSync(path, 'utf-8')) as PoolEntry[]
        for (const e of raw) {
          let p = e.proxy
          if (/^https?:\/\//i.test(p)) {
            merged.push({ ...e, proxy: p })
          } else if (/^socks[45]$/i.test(e.proto ?? '')) {
            merged.push({ ...e, proxy: `${e.proto!.toLowerCase()}://${p}` })
          }
        }
      } catch { /* 忽略坏池文件 */ }
    }
    this.pool = merged
  }

  get size() {
    return this.pool.length
  }

  next(): string | null {
    if (!this.pool.length) return null
    const now = Date.now()
    for (let i = 0; i < this.pool.length; i++) {
      const entry = this.mode === 'round-robin'
        ? this.pool[(this.idx + i) % this.pool.length]
        : this.pool[Math.floor(Math.random() * this.pool.length)]
      const until = this.cooldown.get(entry.proxy)
      if (!until || until < now) {
        this.idx = (this.idx + 1) % this.pool.length
        return entry.proxy
      }
    }
    return null
  }

  reportBad(proxy: string, cooldownMs = 60_000) {
    this.cooldown.set(proxy, Date.now() + cooldownMs)
  }

  reload(poolPaths: string[]) {
    const r = new ProxyRotator(poolPaths, this.mode)
    if (r.size) {
      this.pool = r.pool
      this.cooldown.clear()
      this.idx = 0
    }
  }
}