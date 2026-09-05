/**
 * 并发调度器（框架核心）：任务队列 + 并发窗口 + 重试退避 + 限速 + 稳定性机制。
 *
 * 稳定性保证（用户明确要求项）：
 *   1) 失败重入队：瞬时失败(网络异常/5xx/408/429)在 retries 内退避重试；
 *      重试用尽仍未成功 → 按 requeueDelayMs 指数升级延迟【重新进队】，直到 attempts >= maxAttempts 才判永久失败；
 *      永久失败(4xx 除 408/429)立即终局，不浪费重试。
 *   2) URL 去重：visited 集合记录已抓/在抓的 url，重复 push 直接跳过。
 *   3) 断点/恢复：checkpoint() 把 {queue, visited, failed, stats} 存盘；
 *      restore() 启动时恢复队列与已访问集合（配合 JSONL 输出实现断点续跑）。
 *   4) 失败可见：每次终局失败 emit 'fetch/failed'，供管线写《source>.fails.jsonl 与统计。
 *
 * 解耦：调度器不知道解析/存储；下游只通过事件与 ctx 服务协作。
 */
import { Context, Service } from '@deepseek-ai/cordis'
import type { SchedulerConfig, SourceConfig, SourceTransport } from '../config.ts'
import type { FetchJob, FetchResponse } from '../types.ts'
import { tlog } from '../trace.ts'
import { execFile } from 'node:child_process'
import { join } from 'node:path'
import { fetch as ufetch, ProxyAgent } from 'undici'
import { SocksProxyAgent } from 'socks-proxy-agent'
import { ProxyRotator } from './proxy.ts'

/** 可注入的 HTTP 客户端签名（url → 原始响应【字节】；解码归解析层，支持 GBK 站点） */
export type HttpClient = (url: string, timeoutMs: number) => Promise<{
  status: number
  ok: boolean
  body: Uint8Array | null
}>

/** 单次尝试的结局分类：成功 / 瞬时失败（可重试或重入队）/ 永久失败 */
type Outcome = { kind: 'ok'; res: FetchResponse } | { kind: 'transient'; res?: FetchResponse; err: string }
  | { kind: 'permanent'; res?: FetchResponse; err: string }

interface Job extends FetchJob {
  attempts: number       // 已尝试次数（含重入队）
  requeues: number       // 重入队次数
  batchId?: number       // 所属批次（push 的 id；按批结算用——2026-09-05 多批并发修复）
}

export interface SchedulerStats {
  ok: number
  failed: number         // 永久失败
  requeued: number       // 重入队次数
  skipped: number        // 去重跳过
}

export interface SchedulerSnapshot {
  queue: Job[]
  visited: string[]
  failed: { url: string; source: string }[]
  stats: SchedulerStats
}

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms))

export class SchedulerService extends Service {
  config: SchedulerConfig
  /** 注入点：默认用 Node 全局 fetch；正式环境可换带代理/UA/指纹的客户端 */
  client: HttpClient
  /** 代理池轮换器（config.proxyPool 设置时启用） */
  private rotator: ProxyRotator | null = null
  /** 源级传输策略（2026-09-05：多源共享 fetcher 时每源声明 curl/代理/节奏；key = 源 id） */
  private policies = new Map<string, { transport: SourceTransport; rotator: ProxyRotator | null }>()
  /** 每源最近一次请求开始时间（源级限速节奏） */
  private lastStartBySrc = new Map<string, number>()

  private queue: Job[] = []
  private park: Job[] = []                 // 重入队等待区（延迟未到，尚未回流 queue 的任务）
  private visited = new Set<string>()          // 已抓/在抓/已放弃 的 url（去重）
  private failed = new Map<string, string>()   // 终局失败 url → 所属源（兜底重试轮要用）
  private paused = false                        // 优雅暂停（manage API / pause flag 设置）：停止取新任务，批次尽快结算
  private running = 0
  private loopBusy = false
  private nextId = 0
  private lastFinish = Date.now()              // 最近一次请求完成时间（stall 检测用）
  private last429s: number[] = []              // 近 60s 的 429/503 时间戳（自适应限流）
  private originalConcurrency = 0
  private throttledUntil = 0
  private pending = new Map<number, { jobsLeft: number; done: (v: void) => void; fail: (e: unknown) => void }>()
  readonly stats: SchedulerStats = { ok: 0, failed: 0, requeued: 0, skipped: 0 }

  constructor(ctx: Context, config: SchedulerConfig, sources?: SourceConfig[]) {
    super(ctx, 'scheduler')
    this.config = config
    this.client = this.defaultClient.bind(this)
    if (config.proxyPool || config.proxyPoolSocks) {
      this.setupRotator(null, [config.proxyPool, config.proxyPoolSocks].filter((p): p is string => !!p),
        config.proxyMode ?? 'round-robin', config)
    }
    this.registerSources(sources ?? [])
  }

  /** 装配源级传输策略（ctor 与 watch 热接入共用——晚注册的源补策略；幂等） */
  registerSources(sources: SourceConfig[]): void {
    for (const s of sources ?? []) {
      const t = s.transport
      if (!t || this.policies.has(s.id)) continue
      const files = [t.proxyPool, t.proxyPoolSocks].filter((p): p is string => !!p)
      this.policies.set(s.id, {
        transport: t,
        rotator: files.length ? this.setupRotator(s.id, files, t.proxyMode ?? 'round-robin', this.config) : null,
      })
      tlog({ ev: 'source_transport', source: s.id, curl: t.curlMode ?? false, proxyPool: files.length > 0, delay: t.delayMs })
    }
  }

  /** 建轮换器 + 保鲜刷新（全局池与源级池共用；refresh 间隔/脚本取全局配置） */
  private setupRotator(key: string | null, files: string[], mode: 'round-robin' | 'random', config: SchedulerConfig): ProxyRotator {
    const rot = new ProxyRotator(files.map((p) => join(process.cwd(), p)), mode)
    if (key === null) this.rotator = rot
    tlog({ ev: 'proxy_pool', size: rot.size, mode: rot.mode, source: key ?? 'global' })
    if (!this._refreshArmed) {
      this._refreshArmed = true
      // 保鲜刷新：定时跑 proxyCheckScript（全局配置）并重载全部池（免费代理寿命以分钟计，必须勤刷新）
      // ⚠️ 必须在 try/catch + error 监听下 spawn：沙箱 EPERM 失败只记日志，绝不允许崩掉主进程（血的教训）
      const refreshSec = config.proxyRefreshSec ?? 1200
      if (refreshSec > 0 && config.proxyCheckScript) {
        const script = join(process.cwd(), config.proxyCheckScript)
        const py = config.proxyCheckPython ?? 'python'
        setInterval(() => {
          try {
            tlog({ ev: 'proxy_refresh_start' })
            const child = execFile(py, ['-u', script, '--quick'], { timeout: 10 * 60_000, maxBuffer: 16 * 1024 * 1024 })
            child.on('error', (err) => tlog({ ev: 'proxy_refresh_fail', err: String(err).slice(0, 120) }))
            child.on('exit', (code) => {
              if (code === 0) {
                this.rotator?.reload([config.proxyPool, config.proxyPoolSocks]
                  .filter((p): p is string => !!p).map((p) => join(process.cwd(), p)))
                for (const [, pol] of this.policies) {
                  pol.rotator?.reload([pol.transport.proxyPool, pol.transport.proxyPoolSocks]
                    .filter((p): p is string => !!p).map((p) => join(process.cwd(), p)))
                }
                tlog({ ev: 'proxy_refresh_done' })
              } else {
                tlog({ ev: 'proxy_refresh_fail', code })
              }
            })
          } catch (err) {
            tlog({ ev: 'proxy_refresh_fail', err: String(err).slice(0, 120) })
          }
        }, refreshSec * 1000).unref()
      }
    }
    return rot
  }

  private _refreshArmed = false

  /** 某源的传输策略（无则 null——走全局配置） */
  private policyOf(source: string): { transport: SourceTransport; rotator: ProxyRotator | null } | null {
    return this.policies.get(source) ?? null
  }

  /** 某源的请求间隔（源级 delayMs 优先，回退全局） */
  private delayOf(source: string): number {
    return this.policies.get(source)?.transport.delayMs ?? this.config.delayMs
  }

  /** 心跳：30s 打一次运行快照；在飞请求 >90s 无完成 → stall_warn（区分“慢”与“卡”）
   *  限流恢复：冷却结束且近 60s 无 429 → 恢复并发 */
  private heartbeat = setInterval(() => {
      const idleMs = Date.now() - this.lastFinish
      // 限流恢复：冷却结束且近 60s 无 429 → 恢复并发
      if (this.throttledUntil && Date.now() > this.throttledUntil &&
          (Date.now() - (this.last429s.at(-1) ?? 0) > 60_000)) {
        this.throttledUntil = 0
        this.last429s = []
        if (this.originalConcurrency) this.setConcurrency(this.originalConcurrency)
        tlog({ ev: 'throttle', level: 'restore', concurrency: this.originalConcurrency })
      }
      tlog({
        ev: 'hb',
        queue: this.queue.length, running: this.running, park: this.park.length,
        concurrency: this.config.concurrency, idleMs,
        ok: this.stats.ok, failed: this.stats.failed,
        requeued: this.stats.requeued, skipped: this.stats.skipped,
        stall: this.running > 0 && idleMs > 90_000,
        last429: this.last429s.length,
      })
    }, 30_000).unref()

  /** 运行中调整并发（用户要求：低并发起步、稳定后加码） */
  setConcurrency(n: number) {
    if (!this.originalConcurrency) this.originalConcurrency = this.config.concurrency
    this.config.concurrency = Math.max(1, n)
    tlog({ ev: 'set_concurrency', n: this.config.concurrency })
    void this.loop()
  }

  /** 优雅暂停：停止取新任务，已派发任务继续跑完；未终局任务留队（由调用方 checkpoint 落盘） */
  pause() {
    this.paused = true
    // 批次尽快结算：pending 承诺直接 resolve（在飞任务结束后 finishJob 对其已是 no-op）
    for (const [, h] of this.pending) h.done()
    this.pending.clear()
    tlog({ ev: 'pause_set', queue: this.queue.length, running: this.running })
  }

  isPaused(): boolean {
    return this.paused
  }

  /** 429/503 自适应：60s 内命中过多 → 临时降并发（恢复由心跳检查） */
  private noticeLimited(now: number) {
    this.last429s.push(now)
    while (this.last429s.length && now - this.last429s[0] > 60_000) this.last429s.shift()
    if (this.last429s.length >= 5 && this.config.concurrency > 8) {
      this.throttledUntil = now + 5 * 60_000
      tlog({ ev: 'throttle', level: 'down', concurrency: 8 })
      this.setConcurrency(8)
    }
  }

  /** 默认客户端（代理模式）：
   *  配置 proxyPool 时 = 【纯代理】——每次请求依次尝试 proxyAttempts 个池内出口（失败即冷却换下一个），
   *  池内全部失败 → 抛错交还调度器重试/重入队（【绝不直连兜底】，保全单 IP 不被盾）。
   *  未配置 proxyPool 时 = 直连（常规单出口模式）。
   *  【留桩】浏览器伪装/UA 指纹等在正式源接入时注入。 */
  /** 带硬超时的单次请求（2026-09-05）：TUN 挂起时 abort 不生效 → race 必败承诺兜底。
   *  ⚠️ 硬超时 timer 不能 unref：全进程 timer 都 unref 时事件循环空转，unref timer 不触发，
   *     硬超时会失效（挂死依旧）。ref timer 在请求正常完成后 clearTimeout 清理。
   *  UA：默认带浏览器 UA（2026-09-05 目标限流站 站 444 反爬教训：无 UA 的 undici 请求被 nginx 直接断开） */
  private static readonly UA = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0 Safari/537.36'

  private async fetchHard(url: string, timeoutMs: number, ctrl: AbortController, dispatcher?: any) {
    // connection: close —— 禁用 keep-alive 连接池复用（2026-09-05 目标限流站 站 444 教训：
    // 服务端关闭的死连接被池复用 → nginx 直接 444；python urllib 每请求新连接全通对照验证）
    // referer：同域 origin（2026-09-05 指纹风控站 教训：fiction 页无 referer 一律 403 防盗链——
    // 模拟浏览器站内导航，全站通用无害）
    const headers = {
      'user-agent': SchedulerService.UA,
      connection: 'close',
      referer: new URL(url).origin + '/',
    }
    let hard: ReturnType<typeof setTimeout> | null = null
    const hardP = new Promise<never>((_, rej) => {
      hard = setTimeout(
        () => rej(new Error(`request hard-timeout ${timeoutMs}ms: ${url.slice(0, 90)}`)),
        timeoutMs + 3000)
    })
    // ⚠️ fetch + body 读取必须整体在 race 内（2026-09-05 目标限流站 index 卡死根因）：
    //    TUN 挂起时响应头可达但 body 不来，res.arrayBuffer() 永久挂起——
    //    只 race fetch() 管不到 body 阶段，硬超时形同虚设（连接 ESTABLISHED 空挂）。
    const doFetch = async () => {
      const res = dispatcher
        ? await ufetch(url, { dispatcher, signal: ctrl.signal, redirect: 'follow', headers })
        : await fetch(url, { signal: ctrl.signal, redirect: 'follow', headers })
      const body = res.ok ? new Uint8Array(await res.arrayBuffer()) : null
      return { status: res.status, ok: res.ok, body }
    }
    try {
      return await Promise.race([doFetch(), hardP])
    } finally {
      if (hard) clearTimeout(hard)
    }
  }

  /** 按源取客户端（2026-09-05 多源共享）：源级传输策略优先（curl/代理轮换），
   *  无策略 → 当前 client（注入的自定义 client——自检等；或默认客户端）。
   *  ⚠️ cordis 经 ctx 访问服务时方法/字段会被 getTraceable 逐读包装（新代理）——
   *  切勿对字段做身份比较（2026-09-05 实测 this.client !== this.client）。 */
  private pickClient(sourceId: string): HttpClient {
    const pol = this.policyOf(sourceId)
    const t = pol?.transport
    if (t?.curlMode) {
      return (u, ms) => this.curlClient(u, ms, pol!.rotator)
    }
    if (pol?.rotator) {
      const attempts = t!.proxyAttempts ?? this.config.proxyAttempts ?? 3
      return (u, ms) => this.proxyClient(u, ms, pol.rotator!, attempts, t!.proxyMode)
    }
    return this.client
  }

  private clientFor(job: Job): HttpClient {
    return this.pickClient(job.source)
  }

  /** 旁路抓取（发现/BFS/追更等【不走队列】的调用）：按源路由客户端。
   *  多源共享进程里发现路径必须与正文同策略（curl/代理站拿错客户端 = 全挂，2026-09-05）。 */
  async fetchFor(sourceId: string, url: string, timeoutMs?: number) {
    const client = this.pickClient(sourceId)
    return client(url, timeoutMs ?? this.config.timeoutMs)
  }

  private async defaultClient(url: string, timeoutMs: number) {
    // curl 模式（TLS 指纹风控站）：spawn curl.exe（schannel 指纹放行；node/python OpenSSL 被拒）
    if (this.config.curlMode) {
      return this.curlClient(url, timeoutMs, this.rotator)
    }
    if (this.rotator) {
      return this.proxyClient(url, timeoutMs, this.rotator, this.config.proxyAttempts ?? 3)
    }
    // fetchHard 已读好 body（{status, ok, body}）——勿再 arrayBuffer（2026-09-05 重构残留 bug）
    return this.fetchHard(url, timeoutMs, new AbortController())
  }

  /** 纯代理尝试循环：依次试 proxyAttempts 个池内出口（失败 reportBad 冷却换下一个）；
   *  池内全部失败 → 抛错交调度器重试/重入队（【绝不直连兜底】，保全单 IP 不被盾）。 */
  private async proxyClient(
    url: string, timeoutMs: number, rotator: ProxyRotator,
    attempts: number, _mode?: 'round-robin' | 'random',
  ) {
    const ctrl = new AbortController()
    const timer = setTimeout(() => ctrl.abort(), timeoutMs)
    try {
      for (let i = 0; i < attempts; i++) {
        const p = rotator.next()
        if (!p) break
        try {
          tlog({ ev: 'proxy_try', proxy: p, url })
          const dispatcher: any = p.startsWith('socks')
            ? new SocksProxyAgent(p, { timeout: this.config.timeoutMs })
            : new ProxyAgent(p)
          const res = await this.fetchHard(url, timeoutMs, ctrl, dispatcher)
          return { status: res.status, ok: res.ok, body: res.body }   // fetchHard 已读 body（勿 arrayBuffer）
        } catch (e) {
          rotator.reportBad(p)                          // 连接失败：冷却该出口，继续试下一个
          tlog({ ev: 'proxy_bad', proxy: p, err: String(e) })
        }
      }
      // 池内尝试全部失败：抛错 → 调度器重试/重入队（不直连）
      throw new Error(`proxy pool exhausted: ${url}`)
    } finally {
      clearTimeout(timer)
    }
  }

  /** curl 模式 client：spawn curl.exe（schannel TLS 指纹——指纹风控站放行 curl 拒 node/python）。
   *  -f：4xx/5xx 视为失败（退出码非 0 → 抛错交调度器重试，近似瞬时失败）
   *  -e 同域 referer；proxy 非空时经其轮换代理出口（IP 限速站换 IP 绕过）——
   *  失败/空响应 → reportBad 冷却该代理。输出经 pipe 捕获（maxBuffer 128MB 防大文件截断） */
  private curlClient(url: string, timeoutMs: number, proxy: ProxyRotator | null): Promise<{ status: number; ok: boolean; body: Uint8Array | null }> {
    return new Promise((resolve, reject) => {
      const prx = proxy ? proxy.next() : null
      const args = [
        '-sL', '-f', '--max-time', String(Math.ceil(timeoutMs / 1000) + 5),
        '-A', SchedulerService.UA,
        '-e', new URL(url).origin + '/',
      ]
      if (prx) args.push('-x', prx)
      args.push(url)
      execFile('curl.exe', args, { maxBuffer: 128 * 1024 * 1024, encoding: 'buffer', windowsHide: true },
        (err, stdout) => {
          if (err || !stdout || stdout.length === 0) {
            if (prx) proxy?.reportBad(prx)
            reject(new Error(`curl 失败: ${String(err ?? '空响应').slice(0, 100)}`))
            return
          }
          resolve({ status: 200, ok: true, body: new Uint8Array(stdout as Buffer) })
        })
    })
  }

  /** 入队一批 URL；返回的 Promise 在该批全部【终局】(成功/永久失败)后 resolve
 *  opts.force=true：绕过去重（兜底重试轮补爬用）
 *  opts.front=true：插队到队首（章节展开应立即处理，避免排在整批详情之后） */
  push(urls: string[], source: string, depth = 0, opts?: { continuationOf?: string; force?: boolean; front?: boolean }): Promise<void> {
    const id = this.nextId++
    let jobsLeft = 0
    let resolveFn: (v: void) => void = () => {}
    const p = new Promise<void>((resolve, reject) => {
      resolveFn = resolve
      this.pending.set(id, { jobsLeft: 0, done: resolve, fail: reject })
    })
    for (const url of urls) {
      if (!opts?.force && this.config.dedupe && this.visited.has(url)) {
        this.stats.skipped++
        this.ctx.logger.debug('去重跳过 %s', url)
        continue
      }
      this.visited.add(url)
      const job = {
        url, source, depth, createdAt: Date.now(), attempts: 0, requeues: 0,
        batchId: id,
        continuationOf: opts?.continuationOf,
      }
      if (opts?.front) this.queue.unshift(job)
      else this.queue.push(job)
      jobsLeft++
    }
    tlog({ ev: 'push', n: urls.length, added: jobsLeft, source, forced: opts?.force ?? false, front: opts?.front ?? false })
    const entry = this.pending.get(id)!
    entry.jobsLeft = jobsLeft
    if (jobsLeft === 0) {
      this.pending.delete(id)
      resolveFn()
    } else if (this.paused) {
      // 已暂停：任务留在队里（下次恢复续跑），但批次立即结算——否则调用方 await push 会永久挂起
      this.pending.delete(id)
      resolveFn()
    }
    void this.loop()
    return p
  }

  /** 单飞 worker 循环：队列/并发窗口/限速 → 派发；全部落定后结算批次 */
  private async loop() {
    if (this.loopBusy) return
    this.loopBusy = true
    try {
      while (true) {
        if (this.paused) break
        if (this.queue.length && this.running < this.config.concurrency) {
          await this.dispatchOne()     // 公平取队 + 源级限速（内部可能等待节奏）
        } else if (this.queue.length === 0 && this.running === 0 && this.park.length === 0) {
          this.ctx.logger.debug('scheduler drained')
          void this.checkpoint()               // 批次清空后自动落一次 checkpoint（断点）
          for (const [, h] of this.pending) h.done()
          this.pending.clear()
          break
        } else {
          await sleep(50)
        }
      }
    } finally {
      this.loopBusy = false
    }
  }

  /** 公平派发一个任务（2026-09-05 多源共享）：窗口内优先取【限速已就绪】且【在飞最少】的源的任务——
   *  防大池源饿死小池源（全池 force 入队时队列按源成块，纯 FIFO 会让后面的源等到天荒地老）。
   *  窗口内无就绪任务 → 睡到最早就绪时刻再试。 */
  private runningBySrc = new Map<string, number>()

  private async dispatchOne(): Promise<void> {
    const WINDOW = 64
    const n = Math.min(this.queue.length, WINDOW)
    const now = Date.now()
    let best = -1
    let bestActive = Infinity
    let minWait = Infinity
    for (let i = 0; i < n; i++) {
      const j = this.queue[i]!
      const active = this.runningBySrc.get(j.source) ?? 0
      const wait = this.delayOf(j.source) - (now - (this.lastStartBySrc.get(j.source) ?? 0))
      if (wait <= 0 && active < bestActive) {
        bestActive = active
        best = i
        if (active === 0) break                 // 有空闲源任务就绪 → 不用再找
      }
      if (wait > 0 && wait < minWait) minWait = wait
    }
    if (best < 0) {
      if (minWait < Infinity) await sleep(minWait)
      return
    }
    const [job] = this.queue.splice(best, 1)
    this.lastStartBySrc.set(job.source, Date.now())
    this.running++
    this.runningBySrc.set(job.source, (this.runningBySrc.get(job.source) ?? 0) + 1)
    void this.runOne(job)
      .catch((e) => this.ctx.logger.error('job crashed: %s (%s)', job.url, String(e)))
      .finally(() => {
        this.running--
        const left = (this.runningBySrc.get(job.source) ?? 1) - 1
        if (left <= 0) this.runningBySrc.delete(job.source)
        else this.runningBySrc.set(job.source, left)
        void this.loop()
      })
  }

  /** 单个任务：尝试 → 分类结局 → (重试退避 | 瞬时失败重入队 | 永久失败) */
  private async runOne(job: Job) {
    job.attempts++
    const t0 = Date.now()
    let outcome: Outcome = { kind: 'transient', err: 'unknown' }
    const pol = this.policyOf(job.source)
    const effRetries = pol?.transport.retries ?? this.config.retries
    const effRetryDelay = pol?.transport.retryDelayMs ?? this.config.retryDelayMs
    const effTimeout = pol?.transport.timeoutMs ?? this.config.timeoutMs
    const client = this.clientFor(job)        // 按源路由（curl/代理/注入/全局）

    for (let attempt = 0; attempt <= effRetries; attempt++) {
      if (attempt > 0) {
        const backoff = effRetryDelay * 2 ** (attempt - 1)
        this.ctx.logger.warn('重试 %s（第 %d 次，%dms 后）', job.url, attempt, backoff)
        await sleep(backoff)
      }
      try {
        const r = await client(job.url, effTimeout)
        if (r.status === 429 || r.status === 503 || r.status === 444) {
          // 444 = nginx 无响应（目标限流站 等站反爬/死连接特征）：按限流处理——降并发 + 瞬时重试退避
          this.noticeLimited(Date.now())
        }
        const res: FetchResponse = {
          url: job.url, source: job.source, status: r.status, ok: r.ok && r.body !== null,
          bytes: r.body ? r.body.length : 0, elapsedMs: Date.now() - t0,
          body: r.body, retries: attempt, error: null,
          continuationOf: job.continuationOf ?? null,
        }
        if (res.ok) { outcome = { kind: 'ok', res }; break }
        // 4xx（除 408/429/444）= 永久失败；其余(5xx/网络/444) = 瞬时
        if (r.status >= 400 && r.status < 500 && r.status !== 408 && r.status !== 429 && r.status !== 444) {
          outcome = { kind: 'permanent', res, err: `HTTP ${r.status}` }
          break
        }
        outcome = { kind: 'transient', res, err: `HTTP ${r.status}` }
      } catch (e) {
        outcome = { kind: 'transient', err: String(e) }
        if (attempt >= effRetries) break
      }
    }

    if (outcome.kind === 'ok') {
      this.stats.ok++
      this.lastFinish = Date.now()
      tlog({ ev: 'req_end', url: job.url, source: job.source, ok: true, status: outcome.res?.status ?? 200, ms: Date.now() - t0, attempts: job.attempts })
      await this.ctx.parallel('fetch/response', outcome.res)   // parallel：等待监听器（解析/落盘）完成后才算任务终局
      this.finishJob(job)
      return
    }

    // 瞬时失败且未超总尝试上限 → 重新进队（requeueDelayMs 指数升级；并发窗口已空出）
    if (outcome.kind === 'transient' && job.attempts < this.config.maxAttempts) {
      job.requeues++
      this.stats.requeued++
      const delay = Math.min(this.config.requeueDelayMs * 2 ** job.requeues, this.config.requeueDelayMs * 32)
      this.ctx.logger.warn('瞬时失败重入队 %s（第 %d 次，%dms 后重排）', job.url, job.attempts, delay)
      tlog({ ev: 'requeue', url: job.url, attempts: job.attempts, delay })
      this.park.push(job)                     // 先进等待区：既保证延迟期可见（快照/批次结算），也不会占并发位
      setTimeout(() => {
        const i = this.park.indexOf(job)
        if (i >= 0) this.park.splice(i, 1)
        this.queue.push(job)
        void this.loop()
      }, delay)
      if (outcome.res) this.ctx.emit('fetch/response', outcome.res)   // 让统计侧可见该次失败
      // 注意：批次不在此结算（job 仍属于原批次，终局时结算）
      return
    }

    // 永久失败 / 尝试数耗尽 → 终局（记录来源，供兜底重试轮补爬）
    this.stats.failed++
    this.failed.set(job.url, job.source)
    this.lastFinish = Date.now()
    tlog({ ev: 'fail', url: job.url, source: job.source, err: outcome.err, attempts: job.attempts })
    const failRes: FetchResponse = outcome.res ?? {
      url: job.url, source: job.source, status: 0, ok: false, bytes: 0,
      elapsedMs: Date.now() - t0, body: null, retries: job.attempts, error: outcome.err,
      continuationOf: job.continuationOf ?? null,
    }
    this.ctx.logger.error('终局失败 %s（attempts=%d, %s）', job.url, job.attempts, outcome.err)
    await this.ctx.parallel('fetch/failed', failRes)           // 失败清单写盘完成前不算终局
    await this.ctx.parallel('fetch/response', failRes)
    this.finishJob(job)
  }

  /** 从一个待结算批次里扣减一个终局任务；该批清零才 resolve。
   *  ⚠️ 按【任务所属批次】记账（2026-09-05 多批并发修复）：旧实现每完成一个任务就扣减
   *  全部 pending 批次——多批同时在队时账目全乱，逼得调用方只能逐批 await（整源串行饿死）。 */
  private finishJob(job: Job) {
    if (job.batchId === undefined) return          // restore 回填的任务不属本进程任何批次
    const h = this.pending.get(job.batchId)
    if (!h) return
    h.jobsLeft--
    if (h.jobsLeft <= 0) {
      this.pending.delete(job.batchId)
      h.done()
    }
  }

  /** 兜底重试轮：把终局失败的任务按来源重新入队（绕过去重），返回重试数量。
 *  用户要求：任何级别失败都不能直接放弃 —— 爬取主轮结束后由入口调用。 */
  async retryFailedPass(): Promise<number> {
    const items = [...this.failed.entries()]
    if (!items.length) return 0
    this.failed.clear()
    this.ctx.logger.warn('兜底重试轮：%d 个终局失败重新入队', items.length)
    tlog({ ev: 'fail_retry_pass', n: items.length })
    const bySrc = new Map<string, string[]>()
    for (const [u, s] of items) {
      if (!bySrc.has(s)) bySrc.set(s, [])
      bySrc.get(s)!.push(u)
    }
    for (const [s, urls] of bySrc) {
      await this.push(urls, s, 0, { force: true })
    }
    return items.length
  }

  /** 断点：等待所有任务（含分页续推）完全落定后再返回（自检/收尾用）；暂停后立即返回 */
  async waitIdle() {
    while (!this.paused && (this.queue.length || this.running || this.park.length || this.pending.size)) {
      await sleep(100)
    }
  }

  // ============ 断点：快照 / 恢复 ============

  snapshot(): SchedulerSnapshot {
    return {
      queue: [...this.queue, ...this.park],   // 等待区任务也要进快照（防崩溃丢失）
      visited: [...this.visited],
      failed: [...this.failed].map(([url, source]) => ({ url, source })),
      stats: { ...this.stats },
    }
  }

  restore(snap: SchedulerSnapshot) {
    this.visited = new Set(snap.visited)
    this.failed = new Map(snap.failed.map((f) => [f.url, f.source]))
    // 恢复只回填【未终局】的任务：visited 里没有 failed 的丢回队尾（attempts 保留，沿用 maxAttempts 上限）
    for (const j of snap.queue) {
      if (!this.failed.has(j.url) && !this.visited.has(j.url)) this.visited.add(j.url)
      if (!this.failed.has(j.url)) this.queue.push(j)
    }
    this.stats.ok = snap.stats.ok; this.stats.failed = snap.stats.failed
    this.stats.requeued = snap.stats.requeued; this.stats.skipped = snap.stats.skipped
    this.ctx.logger.info('恢复断点：队内 %d 个未终局任务', this.queue.length)
  }

  /** 把当前队列/已访问/失败清单写入状态文件（进程重启后可 restore 续跑） */
  async checkpoint() {
    const { writeFile, mkdir } = await import('node:fs/promises')
    const { dirname, join } = await import('node:path')
    if (!this.config.stateFile) return
    const file = join(process.cwd(), this.config.stateFile)
    await mkdir(dirname(file), { recursive: true })
    await writeFile(file, JSON.stringify(this.snapshot(), null, 2), 'utf-8')
    this.ctx.logger.debug('checkpoint 已写 %s', this.config.stateFile)
  }
}