/**
 * cordis.yml 装载器：与 DSH（@deepseek-ai/dsh）相同的声明式装配 + 组合 + 热更新（HMR）。
 *
 * ## 格式（顶层 YAML 数组，逐项顺序应用）
 * ```yaml
 * - id: 示例站                      # 条目 id（日志/报错定位用）
 *   name: './sites/示例站.ts'       # 插件：相对路径（相对本文件）/ 包名 / 'cordis:group' / 'cordis:fetcher' / 'cordis:include'
 *   config: { ... }               # 插件配置（站点插件一般放站点专属配置）
 *   disabled: !!js process.platform === 'win32'   # 可选：布尔或 !!js 表达式
 *   group: true                   # 可选：分组（子项放 config 数组里）
 *   isolate: { name: true }       # 可选：cordis 服务隔离（默认无隔离 = root realm 全局共享）
 * ```
 *
 * ## 组合（多文件编排）
 * `cordis:include` 条目把另一个 yml 文件挂进当前树（DSH 同款）：
 * ```yaml
 * - id: sites
 *   name: 'cordis:include'
 *   config:
 *     path: './sites.cordis.yml'      # 子清单（可再嵌套 include）
 *     patches: [...]                  # 可选：DSH PatchOptions 覆盖层（按 id 覆盖/插入条目）
 * - id: fetcher
 *   name: 'cordis:fetcher'
 *   config: { config: './config.json', watch: true }
 * ```
 *
 * ## HMR（热更新）
 * `--cordis app.cordis.yml --watch`（或 mountCordis(path, { watch: true })）：
 * - 监听所有 include 文件（主文件 + 嵌套 include），变化后 300ms 防抖 → 事务性刷新整棵子树
 * - **新增站点条目**（含 config.source）→ 热应用 → 站点插件 emit source/register → fetcher watch 模式自动抓取
 * - 修改站点插件 .ts / 条目配置 → 该条目重启（重新 import + apply，已爬 URL 由 done.urls/visited 去重续爬）
 * - 删除条目 → 优雅卸载（注册的 site.<id> 服务随之注销）
 * - 注意：修改 fetcher 条目自身配置会触发整个 fetcher 重启（断点续跑）；patch 文件变更需重启进程
 *
 * 与 DSH 的差异：相对路径保留 .ts 扩展名（不重写成 .js）——本框架按
 * node --experimental-strip-types 直接加载 TypeScript 插件。
 *
 * 用法：node --experimental-strip-types src/cli.ts --cordis app.cordis.yml [--watch]
 */
import { readFileSync, watch, type FSWatcher } from 'node:fs'
import { dirname, resolve } from 'node:path'
import { fileURLToPath, pathToFileURL } from 'node:url'
import { Context } from '@deepseek-ai/cordis'
import Loader, { Group } from '@deepseek-ai/cordis-plugin-loader'
import Include, { entryListSchema } from '@deepseek-ai/cordis-plugin-include'
import { load as yamlLoad } from 'js-yaml'
import { fetcherPlugin } from './fetcher.ts'

/**
 * Include 的相对路径导入保留 .ts（strip-types 直接加载；DSH 原版把 .ts 重写为 .js，
 * 那是给编译产物用的），并带缓存击穿（?v=时间戳）——热更新后重新 import 能拿到新代码。
 * 一次性类级补丁，root 与嵌套 Include 实例共用。
 */
if (!(Include.prototype as unknown as { __tsImportPatched?: boolean }).__tsImportPatched) {
  const orig = (Include.prototype as unknown as { import: (name: string, stack?: () => string[]) => unknown }).import
  ;(Include.prototype as unknown as {
    import: (name: string, stack?: () => string[]) => unknown
    __tsImportPatched: boolean
  }).import = function (this: Include, name: string, stack?: () => string[]) {
    if (name.startsWith('cordis:')) return this.ctx.loader.builtins[name.slice(7)]
    if (name.startsWith('.')) return import(new URL(name + `?v=${Date.now()}`, this.ctx.baseUrl).href)
    return orig.call(this, name, stack)
  }
  ;(Include.prototype as unknown as { __tsImportPatched: boolean }).__tsImportPatched = true
}

/** cordis.yml 条目（DSH 格式子集） */
export interface CordisEntry {
  id?: string
  /** 插件：相对路径 / 包名 / 'cordis:group' / 'cordis:fetcher' / 'cordis:include' */
  name: string
  /** 插件配置；group/include 条目 = 子条目数组 / 文件配置 */
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

export interface CordisMountOptions {
  /** 热更新：监听 include 文件变化，事务性刷新条目树（配 fetcher watch: true 使用） */
  watch?: boolean
  /** 覆盖层文件（DSH --patch 语义）：每个文件是 PatchOptions 数组（按 id 覆盖/insert 条目） */
  patches?: string[]
}

export interface CordisRunResult {
  app: Context
  /** 是否因暂停标记干净结束（checkpoint 已落盘） */
  paused: boolean
  /** watch 模式的文件监听关闭函数（停止热更新） */
  close?: () => void
}

/** 装载 cordis.yml：挂 Loader 服务 → 注册内建（group/fetcher/include）→ root include 条目装载全部条目。
 *  watch 模式：文件变化热更新；返回 close() 可停止监听。 */
export async function mountCordis(cordisPath: string, opts: CordisMountOptions = {}): Promise<CordisRunResult> {
  const abs = resolve(process.cwd(), cordisPath)
  const app = new Context()
  app.baseUrl = pathToFileURL(dirname(abs)).href + '/'
  await app.plugin(Loader, {})
  app.loader.enableLogs = true
  // 内建插件：'cordis:group'（分组）/ 'cordis:fetcher'（抓取器）/ 'cordis:include'（组合+热更新）
  app.loader.builtins.group = Group
  app.loader.builtins.fetcher = fetcherPlugin
  app.loader.builtins.include = Include

  const patches = loadPatches(opts.patches)
  try {
    // root include：整棵树由主文件驱动（DSH 同款），文件变化 → Include.refresh() 事务性更新
    await app.loader.create({
      id: 'include',
      name: 'cordis:include',
      config: {
        path: pathToFileURL(abs).href,
        ...(patches.length ? { patches } : {}),
      },
    })
  } catch (e) {
    // 条目创建失败（插件 apply 抛错）→ 尝试拆掉已装的条目后抛给上层
    await app.loader.root.stop().catch(() => {})
    throw e
  }
  await app.loader.await()

  // 暂停判定：fetcher 插件对 scheduler 调用过 pause() → 干净结束
  const sched = app.get('scheduler', false) as { isPaused?: () => boolean } | undefined
  const result: CordisRunResult = { app, paused: sched?.isPaused?.() ?? false }
  if (opts.watch) result.close = startWatcher(app)
  return result
}

/** 读取 patch 覆盖层文件（DSH PatchOptions 格式：yaml 数组，按 id 覆盖或 insert 条目） */
function loadPatches(files?: string[]): unknown[] {
  const out: unknown[] = []
  for (const f of files ?? []) {
    const p = resolve(process.cwd(), f)
    const data = yamlLoad(readFileSync(p, 'utf-8'), { schema: entryListSchema }) as unknown
    if (!Array.isArray(data)) throw new Error(`[loader] patch 文件 ${f} 顶层必须是数组（DSH PatchOptions 格式）`)
    out.push(...data)
  }
  return out
}

/** 文件监听热更新：
 *  - include 文件（root + 嵌套）变化 → 事务性刷新该子树（新增/删除/修改条目）
 *  - 相对路径插件文件（站点 .ts）变化 → 强制重启对应条目（重新 import + apply，代码热生效）
 *  全部防抖 300ms；刷新/重启完成后重新收集（可能引入新 include / 新条目）。
 *  注意：fetcher 条目自身配置变更会随 include 刷新触发整树更新（fetcher 重启 = 断点续跑）。 */
function startWatcher(app: Context): () => void {
  const watchers = new Map<string, FSWatcher>()       // dir → watcher
  const timers = new Map<string, ReturnType<typeof setTimeout>>()
  let files = new Map<string, () => Promise<unknown>>()

  const collect = () => {
    const map = new Map<string, () => Promise<unknown>>()
    for (const entry of app.loader.entries()) {
      if (entry.subtree instanceof Include) {
        const inc = entry.subtree
        map.set(inc.filename, () => inc.refresh())
      } else if (typeof entry.options.name === 'string' && entry.options.name.startsWith('.')) {
        // 相对路径插件：文件变化 → force 重启条目（update 无 diff 时 force 也会重新 import + apply）
        let file: string
        try {
          file = fileURLToPath(new URL(entry.options.name, entry.ctx.baseUrl))
        } catch {
          continue
        }
        map.set(file, () => entry.update({}, false, true))
      }
    }
    files = map
  }

  const refreshFile = (file: string) => {
    const act = files.get(file)
    if (!act) return
    if (timers.has(file)) clearTimeout(timers.get(file)!)
    timers.set(file, setTimeout(() => {
      timers.delete(file)
      Promise.resolve(act())
        .then(() => collect())   // 刷新后可能新增/移除 include 或插件条目
        .catch((e) => app.logger.warn('[loader] 热更新失败（保留旧配置）: %s', String(e).slice(0, 200)))
    }, 300))
  }

  const ensureWatchers = () => {
    collect()
    for (const file of files.keys()) {
      const dir = dirname(file)
      if (watchers.has(dir)) continue
      try {
        // persistent：watch 模式 = 进程常驻的锚点（挂起的 promise 不维持事件循环）
        const w = watch(dir, (_ev, fname) => {
          if (typeof fname !== 'string') return
          const full = resolve(dir, fname)
          if (files.has(full)) refreshFile(full)
        })
        w.on('error', (e) => app.logger.warn('[loader] 文件监听错误: %s', String(e).slice(0, 120)))
        watchers.set(dir, w)
      } catch (e) {
        app.logger.warn('[loader] 无法监听 %s: %s', dir, String(e).slice(0, 120))
      }
    }
  }

  ensureWatchers()
  return () => {
    for (const t of timers.values()) clearTimeout(t)
    for (const w of watchers.values()) w.close()
    watchers.clear()
  }
}
