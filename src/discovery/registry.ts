/**
 * 站点处理器注册表（全解耦：框架不认识站点，站点注册到这里，indexer 按 source.siteHandler 分派）。
 * 新增站点 = 写一个 handler（实现 SiteHandler 签名）+ registerSiteHandler 注册一行（各项目在入口注册）。
 */
import type { Context } from '@deepseek-ai/cordis'
import type { SourceConfig } from '../config.ts'

export interface SiteHandler {
  id: string
  discover(ctx: Context, source: SourceConfig): Promise<string[]>
}

const REGISTRY = new Map<string, SiteHandler>()

export function registerSiteHandler(h: SiteHandler) {
  REGISTRY.set(h.id, h)
}

export function getSiteHandler(id: string): SiteHandler | undefined {
  return REGISTRY.get(id)
}