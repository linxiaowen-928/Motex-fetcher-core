/** CLI 入口：
 *   node --experimental-strip-types src/cli.ts [--config <json>] [--phase ...] [--limit N] [--concurrency N]
 *   node --experimental-strip-types src/cli.ts --self-test
 *   node --experimental-strip-types src/cli.ts --cordis <cordis.yml> [--watch] [--patch <file>]...
 */
import { main } from './index.ts'
import { mountCordis } from './loader.ts'

const args = process.argv.slice(2)
const cordisIdx = args.indexOf('--cordis')
const cordisPath = cordisIdx >= 0 && args[cordisIdx + 1] ? args[cordisIdx + 1] : undefined

;(async () => {
  if (cordisPath) {
    const patches: string[] = []
    for (let i = args.indexOf('--patch'); i >= 0; i = args.indexOf('--patch', i + 1)) {
      if (args[i + 1]) patches.push(args[i + 1])
    }
    const watch = args.includes('--watch')
    const { app, paused } = await mountCordis(cordisPath, {
      watch,
      patches: patches.length ? patches : undefined,
    })
    // 暂停 = 干净结束（checkpoint 已落盘）：立即退出，不等待残余定时器
    if (paused) process.exit(0)
    // watch 常驻：进程由文件监听维持；暂停标记 → checkpoint 落盘 → 干净退出
    if (watch && app) {
      await new Promise<void>((r) => app.once('pause/clean', () => r()))
      process.exit(0)
    }
    return
  }
  await main(args)
})().catch((e) => {
  console.error('[motex-fetcher] 致命错误:', e)
  process.exit(1)
})
