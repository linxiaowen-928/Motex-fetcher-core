/**
 * 管线接线插件（对象式插件）：把 调度 → 解析 → 落盘 通过事件串起来。
 *
 * 全解耦的体现：调度器不知道解析器，解析器不知道存储；
 * 本插件只做事件接线：
 *   fetch/response(ok) → parser.parse → fetch/parsed → storage.append
 *   fetch/failed       → storage.appendFail（<source>.fails.jsonl）
 * 各环节可独立替换（换解析器 = 换 ParserService；加去重 = 监听 fetch/parsed 的新插件）。
 */
import type { Context } from '@deepseek-ai/cordis'
import type { FetcherConfig } from '../config.ts'
import type { FetchResponse } from '../types.ts'

export const pipelinePlugin = {
  name: 'pipeline',
  // 注意：不能声明 inject——cordis v4 在 active fiber 内 ctx.plugin() 带 inject 的插件会延迟到父 fiber 结束才 apply（fetcher 插件内装配会错过整个爬取期）；
  // 服务经 ctx.storage 等惰性解析（assembleApp 已先构造服务，事件触发时必然可用）。

  apply(ctx: Context, cfg: FetcherConfig) {
    // 终局失败 → 失败清单落盘（稳定性的可见性保证）
    ctx.on('fetch/failed', async (res: FetchResponse) => {
      ctx.logger.warn('失败清单：%s（%s）', res.url, res.error ?? `HTTP ${res.status}`)
      await ctx.storage.appendFail(res)
    })

    ctx.on('fetch/response', async (res: FetchResponse) => {
      if (!res.ok) {
        return   // 失败路径由 fetch/failed 处理；这里只处理成功
      }
      const source = cfg.sources.find((s) => s.id === res.source)
      if (!source) {
        ctx.logger.warn('未知来源 %s，跳过解析', res.source)
        return
      }
      if (source.downloadRaw) {
        // 二进制直落盘（音频/压缩包等）：不解析，存文件 + 元数据
        await ctx.storage.saveBinary(res)
        return
      }
      for (const item of ctx.parser.parse(res, source)) {
        ctx.emit('fetch/parsed', res, item)
        await ctx.storage.append(item)
      }
    })
  },
}