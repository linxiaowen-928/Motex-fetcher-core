/**
 * cordis.yml 装载器：与 DSH（@deepseek-ai/dsh）相同的声明式装配格式。
 *
 * 格式（顶层 YAML 数组，逐项顺序应用）：
 * ```yaml
 * - id: 示例站                      # 条目 id（日志/报错定位用）
 *   name: './sites/示例站.ts'       # 插件：相对路径（相对本文件）/ 包名 / 'cordis:group' / 'cordis:fetcher'
 *   config: { ... }               # 插件配置（站点插件一般放站点专属配置）
 *   disabled: !!js process.platform === 'win32'   # 可选：布尔或 !!js 表达式
 *   group: true                   # 可选：分组（子项放 config 数组里）
 *   isolate: { name: true }       # 可选：cordis 服务隔离（默认无隔离 = root realm 全局共享）
 * ```
 *
 * 与 DSH 的差异：相对路径保留 .ts 扩展名（不重写成 .js）——本框架按
 * node --experimental-strip-types 直接加载 TypeScript 插件。
 *
 * 用法：node --experimental-strip-types src/cli.ts --cordis app.cordis.yml
 */
import { readFileSync } from 'node:fs'
import { dirname, resolve } from 'node:path'
import { pathToFileURL } from 'node:url'
import { Context } from '@deepseek-ai/cordis'
import Loader, { Group } from '@deepseek-ai/cordis-plugin-loader'
import { parse } from 'yaml'
import { fetcherPlugin } from './fetcher.ts'

/** yaml 的 !!js 标签：解析为 { __jsExpr }，由 loader 在运行上下文求值（与 DSH 一致） */
const JS_TAG = {
  tag: 'tag:yaml.org,2002:js',
  resolve: (value: string) => ({ __jsExpr: value }),
}

/** cordis.yml 条目（DSH 格式子集） */
export interface CordisEntry {
  id?: string
  /** 插件：相对路径 / 包名 / 'cordis:group' / 'cordis:fetcher' */
  name: string
  /** 插件配置；group 条目 = 子条目数组 */
  config?: unknown
  /** 布尔或 !!js 表达式（禁用条目不加载） */
  disabled?: unknown
  /** 分组标记（配合 name: 'cordis:group'） */
  group?: boolean
  /** 服务隔离（可选；缺省 = root realm，全树共享） */
  isolate?: Record<string, unknown>
  /** 插件注入声明（DSH 兼容字段，可选） */
  inject?: Record<string, unknown>
}

export interface CordisRunResult {
  app: Context
  /** 是否因暂停标记干净结束（checkpoint 已落盘） */
  paused: boolean
}

/** 装载 cordis.yml：挂 Loader 服务 → 注册内建（group/fetcher）→ 按序创建条目 → 等待全部插件落定。
 *  返回 app 与暂停状态（CLI 据此决定是否立即退出）。 */
export async function mountCordis(cordisPath: string): Promise<CordisRunResult> {
  const abs = resolve(process.cwd(), cordisPath)
  const app = new Context()
  app.baseUrl = pathToFileURL(dirname(abs)).href + '/'
  await app.plugin(Loader, {})
  app.loader.enableLogs = true
  // 内建插件：'cordis:group'（分组）/ 'cordis:fetcher'（抓取器）
  app.loader.builtins.group = Group
  app.loader.builtins.fetcher = fetcherPlugin
  // 相对路径保留 .ts（strip-types 直接加载）；其余交给 DSH 同款解析逻辑
  app.loader.import = async (name: string) => {
    if (name.startsWith('cordis:')) return app.loader.builtins[name.slice(7)]
    if (name.startsWith('.')) return import(new URL(name, app.baseUrl).href)
    return import(name)
  }

  const text = readFileSync(abs, 'utf-8')
  const entries = parse(text, { customTags: [JS_TAG] }) as CordisEntry[]
  if (!Array.isArray(entries)) {
    throw new Error(`[loader] ${cordisPath} 顶层必须是 YAML 数组（DSH 格式：- id / name / config ...）`)
  }

  try {
    for (const entry of entries) {
      if (!entry || typeof entry.name !== 'string') {
        throw new Error(`[loader] ${cordisPath} 条目缺少 name（每条应为 { id?, name, config?, ... }）`)
      }
      // 逐条创建并等待：站点插件先于 fetcher 条目（先提供 site.<id>，fetcher 装配后即可 DI 分派）
      await app.loader.create(entry)
    }
  } catch (e) {
    // 条目创建失败（插件 apply 抛错）→ 尝试拆掉已装的条目后抛给上层
    await app.loader.root.stop().catch(() => {})
    throw e
  }
  await app.loader.await()

  // 暂停判定：fetcher 插件对 scheduler 调用过 pause() → 干净结束
  const sched = app.get('scheduler', false) as { isPaused?: () => boolean } | undefined
  return { app, paused: sched?.isPaused?.() ?? false }
}
