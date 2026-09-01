/**
 * 类型与事件契约：全框架的“解耦接口”都在这层定义。
 * - 服务：scheduler / indexer / parser / storage（挂到 ctx 上，按名注入）
 * - 事件：fetch/request → fetch/response → fetch/parsed（管线各环节通过事件解耦）
 * 说明：cordis 是「作用域 DI + 生命周期副作用 + 事件」的插件框架，
 * 各组件之间不直接互相 new，只通过 ctx 上注册的服务与事件协作。
 */
import type { Context } from '@deepseek-ai/cordis'
import type { SourceConfig } from './config.ts'

declare module '@deepseek-ai/cordis' {
  interface Context {
    scheduler: import('./services/scheduler.ts').SchedulerService
    indexer: import('./services/indexer.ts').IndexerService
    parser: import('./services/parser.ts').ParserService
    storage: import('./services/storage.ts').StorageService
  }

  interface Events {
    /** 调度器准备抓取一个 URL（含来源与深度信息） */
    'fetch/request'(job: FetchJob): void
    /** 一个 URL 抓取完成（成功或失败均派发），附来源标识 */
    'fetch/response'(res: FetchResponse): void
    /** 一个 URL 终局失败（永久失败或尝试数耗尽），供统计/告警/失败清单写入 */
    'fetch/failed'(res: FetchResponse): void
    /** 一条解析出的正文条目落盘前派发（供后续扩展：去重/筛选/入库） */
    'fetch/parsed'(res: FetchResponse, item: ParsedItem): void
    /** 站点插件注册源配置（运行中热接入：fetcher watch 模式监听后自动抓取该源） */
    'source/register'(source: SourceConfig): void
    /** 优雅暂停完成（checkpoint 已落盘、调度器已停止）——watch 常驻模式的干净退出信号 */
    'pause/clean'(): void
  }
}

/** 一次抓取任务的描述 */
export interface FetchJob {
  url: string
  source: string        // 来源 id（对应 SourceConfig.id）
  depth: number         // 深度（第 0 层 = 种子页），后续广搜时使用
  createdAt: number
  /** 内容分页跟随：该页是某篇小说的第 N 页（值为规范化后的小说基准 URL） */
  continuationOf?: string
}

/** 一次抓取的原始结果 */
export interface FetchResponse {
  url: string
  source: string
  status: number
  ok: boolean
  bytes: number
  elapsedMs: number
  /** 原始响应字节（解码交给解析层，按站点规则 encoding 处理，兼容 GBK 站点） */
  body: Uint8Array | null
  retries: number
  error: string | null
  /** 内容分页跟随标记（从任务透传；解析器据此拼接分页正文） */
  continuationOf?: string | null
}

/** 索引池条目（两阶段模式：索引阶段先收集，爬取阶段从池取） */
export interface IndexRecord {
  url: string
  title?: string
  source: string
  catId?: number
  /** 站点分类/列表标识（如 'all'、'last_published'；与 catId 二选一按站点） */
  catKey?: string
  page?: number
  /** 是否连载中（true=需要追更；站点无此信息时不填） */
  ongoing?: boolean
}

/** 解析/清洗后的正文条目（最终落盘 JSONL 的一行） */
export interface ParsedItem {
  url: string
  source: string
  text: string
  title?: string
  meta?: Record<string, unknown>
}

/** 站点处理器契约：项目插件通过 ctx.provide('site.<id>', handler) 注册，indexer 经 cordis DI 分派 */
export interface SiteHandler {
  id: string
  discover(ctx: Context, source: SourceConfig): Promise<string[]>
}
