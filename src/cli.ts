/** CLI 入口：
 *   node --experimental-strip-types src/cli.ts [--config <json>] [--phase ...] [--limit N] [--concurrency N]
 *   node --experimental-strip-types src/cli.ts --self-test
 *   node --experimental-strip-types src/cli.ts --cordis <cordis.yml>   # DSH 式声明式装配（推荐）
 */
import { main } from './index.ts'
import { mountCordis } from './loader.ts'

const args = process.argv.slice(2)
const cordisIdx = args.indexOf('--cordis')
const cordisPath = cordisIdx >= 0 && args[cordisIdx + 1] ? args[cordisIdx + 1] : undefined

;(async () => {
  if (cordisPath) {
    const { paused } = await mountCordis(cordisPath)
    // 暂停 = 干净结束（checkpoint 已落盘）：立即退出，不等待残余定时器
    if (paused) process.exit(0)
    return
  }
  await main(args)
})().catch((e) => {
  console.error('[motex-fetcher] 致命错误:', e)
  process.exit(1)
})
