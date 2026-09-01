/**
 * 配置契约与默认值。
 *
 * sources：数据源列表。【留桩】源相关字段（indexRule/parseRule）等具体站点确认后扩展；
 * scheduler：并发/重试/限速等调度参数（框架已实现）；
 * storage：落盘位置（JSONL）。
 * 待用户提供具体站点后，主要工作 = 为对应源实现 indexer 与 parser 的规则（见插件目录注释）。
 */
import { readFileSync } from 'node:fs'
import type { SiteIndexRule, SiteParseRule } from './rules.ts'

export interface SourceConfig {
  /** 源标识：如 'siteA'，贯穿 job/response/item 的 source 字段 */
  id: string
  /** 源类型：static=固定 URL 列表；index=抓目录页按 linkRegex 发现 URL；site=走站点处理器（多级索引/过滤） */
  kind: 'static' | 'index' | 'site'
  /** 种子页 URL（static 直接用；index 作为索引起点；site 交给处理器） */
  seedUrls: string[]
  /** 站点处理器 id（kind='site' 必填；注册表见 src/sites/index.ts） */
  siteHandler?: string
  /** 站点特有配置（两级分类/分页/关键字过滤等，由处理器解释） */
  handlerConfig?: Record<string, unknown>
  /** 小说标题关键字过滤：标题命中任一关键字的小说跳过不抓（从配置添删） */
  filterKeywords?: string[]
  /** 索引规则：目录页 linkRegex 提取章节链接（index 源适用） */
  indexRule?: SiteIndexRule
  /** 正文解析规则：编码/选择器/广告行过滤/标题/最小长度（见 rules.ts） */
  parseRule?: SiteParseRule
  /** 二进制直落盘（音频/压缩包等：跳过解析，按 url 哈希存文件 + 元数据 jsonl） */
  downloadRaw?: boolean
}

export interface SchedulerConfig {
  /** 最大并发连接数 */
  concurrency: number
  /** 单 URL 每次尝试内的重试次数（net/5xx 指数退避） */
  retries: number
  /** 重试基础退避（毫秒），指数退避 = base * 2^n */
  retryDelayMs: number
  /** 瞬时失败【重新进队】的基础延迟（毫秒），按重入次数指数升级（封顶 32 倍） */
  requeueDelayMs: number
  /** 单个 URL 的总尝试次数上限（含重试与重入队），达到即终局失败 */
  maxAttempts: number
  /** 是否去重（visited 集合跳过重复 URL） */
  dedupe: boolean
  /** 断点状态文件（相对运行目录，如 state/run.json）；空 = 不落盘 */
  stateFile: string
  /** 爬取主轮结束后，对终局失败再跑的兜底重试轮数（默认 1：失败不能不管） */
  failRetryPasses?: number
  /** 代理池文件（proxy_check.py 产出 pool.json 的路径；设置后 client 走【纯代理】模式，绝不直连兜底） */
  proxyPool?: string
  /** 附加 socks 代理池（proxy_check_socks.py 产出 pool_socks.json；与 proxyPool 合并轮换） */
  proxyPoolSocks?: string
  /** 代理轮换模式：round-robin（默认）/ random */
  proxyMode?: 'round-robin' | 'random'
  /** 单请求最多尝试的池内出口数（默认 3；全失败交回调度器重试/重入队） */
  proxyAttempts?: number
  /** 代理池保鲜刷新周期（秒；>0 且配置 proxyCheckScript 时定时刷新，默认 1200） */
  proxyRefreshSec?: number
  /** 代理保鲜脚本路径（如 proxy_check.py；不配置则只加载不刷新） */
  proxyCheckScript?: string
  /** 两次请求间最小间隔（毫秒，礼貌限速） */
  delayMs: number
  /** 单请求超时（毫秒） */
  timeoutMs: number
  /** 【留桩】代理（如 'http://user:pass@host:port'），需要时用 undici ProxyAgent 接入 */
  proxy?: string
}

export interface StorageConfig {
  /** JSONL 输出目录 */
  outDir: string
  /** 落盘时剥离文本中的换行（训练语料不需要段落 \n；默认开） */
  stripNewlines?: boolean
}

export interface FetcherConfig {
  sources: SourceConfig[]
  scheduler: SchedulerConfig
  storage: StorageConfig
  /** 运行阶段：index=只建索引池；crawl=只从池爬正文；both=两者（默认） */
  phase?: 'index' | 'crawl' | 'both'
  /** 索引池 JSONL 文件（phase=index/crawl 时使用；相对运行目录） */
  indexFile?: string
  /** crawl 阶段跳过已在输出 JSONL 中存在的 url（断点式续爬） */
  skipExisting?: boolean
}

export const DEFAULT_CONFIG: FetcherConfig = {
  sources: [],
  phase: 'both',
  /** 索引池放在独立 pool/ 目录（state/ 会被开发清理误删，池子是资产不能丢） */
  indexFile: 'pool/index.jsonl',
  skipExisting: true,
  scheduler: {
    concurrency: 8, retries: 2, retryDelayMs: 1000, requeueDelayMs: 10000,
    maxAttempts: 5, dedupe: true, stateFile: 'state/run.json', failRetryPasses: 1,
    proxyRefreshSec: 1200,
    delayMs: 200, timeoutMs: 30000,
  },
  storage: { outDir: 'out', stripNewlines: true },
}

/** 读取并合并 JSON 配置（简单实现；需要时可换 cordis 官方的 loader/配置文件支持） */
export function loadConfig(path?: string): FetcherConfig {
  const cfg: FetcherConfig = JSON.parse(JSON.stringify(DEFAULT_CONFIG))
  if (path) {
    const user = JSON.parse(readFileSync(path, 'utf-8')) as Partial<FetcherConfig>
    if (user.sources) cfg.sources = user.sources
    if (user.phase) cfg.phase = user.phase
    if (user.indexFile) cfg.indexFile = user.indexFile
    if (user.skipExisting !== undefined) cfg.skipExisting = user.skipExisting
    if (user.scheduler) Object.assign(cfg.scheduler, user.scheduler)
    if (user.storage) Object.assign(cfg.storage, user.storage)
  }
  // 多站点并行隔离：不显式配置时按第一个源 id 自动分目录（索引池/断点互不覆盖）
  const sid = cfg.sources[0]?.id
  if (sid) {
    if (!cfg.indexFile || cfg.indexFile === DEFAULT_CONFIG.indexFile) {
      cfg.indexFile = `pool/${sid}.index.jsonl`
    }
    if (!cfg.scheduler.stateFile || cfg.scheduler.stateFile === DEFAULT_CONFIG.scheduler.stateFile) {
      cfg.scheduler.stateFile = `state/${sid}/run.json`
    }
  }
  return cfg
}