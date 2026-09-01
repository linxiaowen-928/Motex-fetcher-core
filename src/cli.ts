/** CLI 入口：node --experimental-strip-types src/cli.ts [--config ...] [--self-test] */
import { main } from './index.ts'
main(process.argv.slice(2)).catch((e) => {
  console.error('[motex-fetcher] 致命错误:', e)
  process.exit(1)
})