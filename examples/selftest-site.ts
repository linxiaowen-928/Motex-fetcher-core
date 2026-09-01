/**
 * 站点插件示例（cordis 原生形式）：一个插件 = 一个站点的接入。
 *
 * 通过 ctx.provide('site.<id>', handler) 注册站点处理器，indexer 在发现阶段经
 * cordis 作用域 DI（ctx.get('site.<id>')）分派到本插件——与 fetcher 插件装配的
 * 核心服务在同一上下文树里共享。
 *
 * 本文件是自检专用（配合 selftest.cordis.yml）：模拟"发现 3 本 → 关键字过滤掉 1 本"。
 * 真实站点插件结构相同，discover 里换成该站点的目录页抓取/解析逻辑即可。
 */
import { Context } from '@deepseek-ai/cordis'
import type { SiteHandler, SourceConfig } from '../src/types.ts'

export default function selftestSitePlugin(ctx: Context) {
  ctx.provide('site.selftest-site', {
    id: 'selftest-site',
    discover: async (_ctx: Context, source: SourceConfig) => {
      const kws = source.filterKeywords ?? []
      const found = [
        { url: 'https://fake.local/n1', title: '正常小说标题' },
        { url: 'https://fake.local/n2', title: '含有禁词的小说标题' },
        { url: 'https://fake.local/n3', title: '另一本正常小说' },
      ]
      // 命中任一禁词的小说跳过不抓（用户要求的敏感词过滤语义）
      return found.filter((n) => !kws.some((k) => n.title.includes(k))).map((n) => n.url)
    },
  } satisfies SiteHandler)
}
