/**
 * 网站规则引擎（站点实例化的核心）：
 * 给一个站点提供 {catalog/parse 规则}，即可让 indexer/parser 直接执行，无需改框架代码。
 *
 * 规则形态（config 里 sources[].indexRule / parseRule 直接填）：
 *   indexRule: {
 *     linkRegex: '章节链接提取正则（对目录页 HTML 全文匹配，第 1 个捕获组为 URL）',
 *   }
 *   parseRule: {
 *     encoding:  'utf-8' | 'gbk'，            // 正文页编码（中文站点常见 GB2312/GBK）
 *     bodyStart: '正文起始锚串（不含则从头）',
 *     bodyEnd:   '正文结束锚串（不含则到结尾）',
 *     titleRegex: '标题提取正则（可选）',
 *     adLineRegex: ['广告/杂物行过滤正则（逐行匹配即删，可多条；可复用 AD_RE 的思路）'],
 *     minLen:     40,                         // 低于该长度的正文丢弃
 *   }
 */
import * as cheerio from 'cheerio'

export interface SiteIndexRule {
  /** 目录页 HTML 全文上提取章节链接的正则；第 1 个捕获组必须是 URL（相对路径会拼成绝对路径） */
  linkRegex: string
}

export interface SiteParseRule {
  /** 页面编码：utf-8 / gbk（Node 的 TextDecoder 需 full-icu，官方构建自带） */
  encoding?: 'utf-8' | 'gbk'
  /** 【选择器式】正文所在容器（CSS 选择器，如 '#wznr'）；与锚串式二选一 */
  section?: string
  /** 【选择器式】标题元素（CSS 选择器，如 '.nrtitle h1'，可选） */
  titleSelector?: string
  /** 【选择器式】正文字块（CSS 选择器，如 '.ttnr'；缺省取 section 内去掉标题后的全部） */
  contentSelector?: string
  /** 正文起始锚串 */
  bodyStart?: string
  /** 正文结束锚串 */
  bodyEnd?: string
  /** 标题提取正则（第 1 捕获组为标题） */
  titleRegex?: string
  /** 逐行广告/杂物过滤正则（命中即删行） */
  adLineRegex?: string[]
  /** 正文最短长度（字符），低于则丢弃 */
  minLen?: number
  /** 内容分页跟随：检测正文区内“下一页/下页/数字分页”链接并自动拼接（防御能力；站点无需时关掉） */
  followPagination?: boolean
  /** 详情页信号（页面含此子串时为“目录/详情页”）：解析章节链接并入队，页面本身不入库 */
  chapterSignal?: string
  /** 章节链接过滤前缀（如 '/fiction/id-'）；缺省取所有链接 */
  chapterLinkPrefix?: string
  /** 提取前从正文容器移除的选择器（清洗站点导航/按钮 UI，如 ['.fiction-chapter-navigator']） */
  removeSelectors?: string[]
}

export const DEFAULT_MIN_LEN = 40

/** 从 HTML 中提取所有链接（相对 → 绝对） */
export function extractLinks(html: string, linkRegex: RegExp, baseUrl: string): string[] {
  const out: string[] = []
  let m: RegExpExecArray | null
  while ((m = linkRegex.exec(html)) !== null) {
    let u = m[1]
    if (!u) continue
    u = u.trim().replace(/&amp;/g, '&')
    if (!/^https?:\/\//i.test(u)) {
      u = new URL(u, baseUrl).href          // 相对路径拼绝对
    }
    out.push(u)
  }
  return out
}

/** 按规则抽取正文（锚串截取 → 去标签 → 行级广告过滤 → 最小长度检查） */
export function extractText(
  html: string,
  rule: SiteParseRule,
  linkRegex?: RegExp,
): { title?: string; text: string } | null {
  let seg = html
  if (rule.bodyStart) {
    const i = seg.indexOf(rule.bodyStart)
    if (i < 0) return null                  // 找不到正文锚点 → 整页判废（弱保护）
    seg = seg.slice(i + rule.bodyStart.length)
  }
  if (rule.bodyEnd) {
    const j = seg.indexOf(rule.bodyEnd)
    if (j >= 0) seg = seg.slice(0, j)
  }
  const text = seg
    .replace(/<script[\s\S]*?<\/script>/gi, ' ')
    .replace(/<style[\s\S]*?<\/style>/gi, ' ')
    .replace(/<br\s*\/?>/gi, '\n')
    .replace(/<\/p>|<\/div>|<\/h[1-6]>/gi, '\n')
    .replace(/<[^>]+>/g, ' ')
    .replace(/&nbsp;/g, ' ').replace(/&lt;/g, '<').replace(/&gt;/g, '>')
    .replace(/&amp;/g, '&').replace(/&quot;/g, '"').replace(/&#\d+;/g, ' ')
    .replace(/[ \t]+/g, ' ')
    .replace(/\n{3,}/g, '\n\n')
    .trim()

  // 行级广告/杂物过滤（命中即删行）——沿用 Python 侧 AD_RE 的思路：站名/网址/“最新章节请…”等模板行
  const adRe = rule.adLineRegex
  const lines = text.split('\n')
  const kept = adRe?.length
    ? lines.filter((ln) => !adRe.some((pat) => new RegExp(pat).test(ln)))
    : lines
  const clean = kept.join('\n').trim()

  const minLen = rule.minLen ?? DEFAULT_MIN_LEN
  if (clean.length < minLen) return null

  let title: string | undefined
  if (rule.titleRegex) {
    const tm = new RegExp(rule.titleRegex).exec(html)
    if (tm?.[1]) title = tm[1].trim()
  }
  return { title, text: clean }
}

/** 选择器式正文抽取（cheerio）：section → 标题(titleSelector) + 内容(contentSelector) → 纯文本 */
export function extractWithSelectors(html: string, rule: SiteParseRule) {
  const $ = cheerio.load(html)
  const sec = rule.section ? $(rule.section) : $('body')
  if (!sec.length || !sec.first().html()) return null
  const title = rule.titleSelector ? sec.find(rule.titleSelector).first().text().trim() : undefined
  let contentHtml: string | undefined
  if (rule.contentSelector) {
    const c = sec.find(rule.contentSelector).first()
    contentHtml = c.html() ?? undefined
  } else {
    // 无 contentSelector：section 内部去掉标题元素后的 HTML
    const copy = sec.clone()
    if (rule.titleSelector) copy.find(rule.titleSelector).first().remove()
    // 清洗站点导航/按钮 UI（“上一章/下一章/100% 阅读了” 等）
    for (const sel of rule.removeSelectors ?? []) {
      copy.find(sel).remove()
    }
    contentHtml = copy.html() ?? undefined
  }
  if (!contentHtml) return null

  const text = htmlToLines(contentHtml)
  const adRe = rule.adLineRegex
  const kept = adRe?.length ? text.filter((ln) => !adRe.some((pat) => new RegExp(pat).test(ln))) : text
  const clean = kept.join('\n').trim()
  const minLen = rule.minLen ?? DEFAULT_MIN_LEN
  if (clean.length < minLen) return null
  return { title: title || undefined, text: clean }
}

/** HTML 片段 → 按段落拆行（保留 <p>/<br>/<div>/<h*> 产生的换行） */
function htmlToLines(html: string): string[] {
  const t = html
    .replace(/<script[\s\S]*?<\/script>/gi, ' ')
    .replace(/<style[\s\S]*?<\/style>/gi, ' ')
    .replace(/<br\s*\/?>/gi, '\n')
    .replace(/<\/p>|<\/div>|<\/h[1-6]>|<\/li>/gi, '\n')
    .replace(/<[^>]+>/g, ' ')
    .replace(/&nbsp;/g, ' ').replace(/&lt;/g, '<').replace(/&gt;/g, '>')
    .replace(/&amp;/g, '&').replace(/&quot;/g, '"').replace(/&#\d+;/g, ' ')
    .replace(/[ \t]+/g, ' ')
    .replace(/\n{3,}/g, '\n\n')
  return t.split('\n').map((s) => s.trim()).filter(Boolean)
}

/** 检测内容分页的“下一页”链接（在 section 区域内找）：
 *  1) 文本含 下一页/下页 的链接 → 直接采用；
 *  2) 兜底：形如 id/xxx_N.html 的数字分页链接（取 _N>1 中最小者近似“下一页”，启发式）。
 *  返回绝对 URL；无则 null。 */
export function detectNextPage(html: string, section?: string, baseUrl?: string): string | null {
  const $ = cheerio.load(html)
  const scope = section ? $(section) : $('body')
  let nextText: string | null = null
  let numFallback: string | null = null
  scope.find('a').each((_, el) => {
    const $a = $(el)
    const txt = $a.text().trim()
    const href = $a.attr('href') ?? ''
    if (!href) return
    const abs = baseUrl && !/^https?:\/\//i.test(href) ? new URL(href, baseUrl).href : href
    if (/(下一页|下页)/.test(txt) && !nextText) {
      nextText = abs
    } else if (!nextText && /_\d{1,3}\.html$/i.test(abs)) {
      const m = /_(\d{1,3})\.html$/i.exec(abs)
      if (m && Number(m[1]) > 1 && !numFallback) numFallback = abs
    }
  })
  return nextText ?? numFallback
}

/** 规范化：分页 URL → 小说基准 URL（id/200805_2.html → id/200805.html；pg_2.html → pg.html） */
export function normalizeBase(url: string): string {
  return url.replace(/_\d{1,3}\.html$/i, '.html')
}

/** 按编码解码字节（utf-8 / gbk） */
export function decodeBytes(bytes: Uint8Array, encoding?: 'utf-8' | 'gbk'): string {
  try {
    return new TextDecoder(encoding === 'gbk' ? 'gbk' : 'utf-8').decode(bytes)
  } catch (e) {
    // gbk 解码器不可用（非 full-icu 构建）时退回 utf-8
    return new TextDecoder('utf-8').decode(bytes)
  }
}