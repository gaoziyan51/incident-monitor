/**
 * wecom-patrol.mjs —— 纯云端重大事件巡检推送（GitHub Actions 定时运行，不依赖本机）
 *
 * 逻辑：抓取谷歌新闻 RSS（国内灾害/事故源）→ 关键词匹配"重大人身伤亡/重大自然灾害"
 *      → 三层真实性把关 → 按模板推送企业微信群 → 状态文件去重
 *
 * 三层把关：
 *  1. 官方媒体白名单：来源 site 必须是央视/新华网/人民网/中国政府网等官方媒体
 *  2. 多源印证：同一事件（标题相似分组）被 ≥2 家不同媒体报道，可信度更高
 *  3. 评论类排除：视频｜/评论/警示/启示/盘点/解读等标题一律不推
 *
 * 消息不提供原文直链（谷歌聚合源拿不到），标注"来源渠道+可按标题检索核实"，
 * 详情链接 = 简报主页。
 *
 * 运行：node scripts/wecom-patrol.mjs （需环境变量 WECOM_WEBHOOK）
 */
import { createHash } from 'node:crypto';
import { readFileSync, writeFileSync, mkdirSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = dirname(fileURLToPath(import.meta.url));
const STATE_FILE = join(__dirname, '..', 'data', 'wecom-patrol-state.json');
const WEBHOOK = (process.env.WECOM_WEBHOOK || '').trim();
const BRIEF_URL = 'https://liudi7675.github.io/incident-monitor/';
const MAX_PUSH = 3;          // 每轮最多推送条数（防刷屏）
const FRESH_HOURS = 26;      // 只推最近 N 小时内发布的消息（防旧闻）
const STATE_TTL_MS = 7 * 86400000; // 去重状态保留7天

if (!WEBHOOK) {
  console.log('未配置 WECOM_WEBHOOK，跳过推送');
  process.exit(0);
}

/* ---------------- 数据源（国内灾害/事故，与 fetch-news.mjs 同源） ---------------- */
const Q = (q) => 'https://news.google.com/rss/search?q=' + encodeURIComponent(q);
const CN = '&hl=zh-CN&gl=CN&ceid=CN:zh-Hans';
const SOURCES = [
  { name: 'natural', url: Q('暴雨 OR 洪涝 OR 山洪 OR 台风 OR 龙卷风 OR 泥石流 OR 山体滑坡 OR 滑坡 OR 崩塌 OR 塌方 OR 地震 OR 溃坝 OR 坍塌 OR 倒塌') + CN },
  { name: 'accident', url: Q('火灾 OR 爆炸 OR 燃爆 OR 踩踏 OR 坠机 OR 空难 OR 沉船 OR 翻船 OR 矿难 OR 透水 OR 危化品事故 OR 起火') + CN },
  { name: 'casualty', url: Q('(遇难 OR 失联 OR 失踪 OR 伤亡 OR 被困) (洪水 OR 台风 OR 地震 OR 泥石流 OR 滑坡 OR 火灾 OR 爆炸 OR 矿难 OR 事故 OR 灾害)') + CN },
  { name: 'response', url: Q('"Ⅰ级响应" OR "Ⅱ级响应" OR 国家防总 OR 特大自然灾害 OR 重大事故 OR 国务院工作组 OR 国家消防救援局') + CN },
];

/* ---------------- 过滤规则 ---------------- */
/* 境外地名（命中即视为国外事件不推） */
const FOREIGN_RE = /(尼泊尔|不丹|孟加拉|斯里兰卡|马尔代夫|巴基斯坦|印度尼西亚|印尼|印度|日本|韩国|朝鲜|蒙古|越南|老挝|柬埔寨|泰国|缅甸|马来西亚|新加坡|菲律宾|文莱|东帝汶|哈萨克斯坦|乌兹别克斯坦|吉尔吉斯|塔吉克斯坦|土库曼斯坦|阿富汗|伊朗|伊拉克|叙利亚|黎巴嫩|约旦|以色列|巴勒斯坦|沙特|阿联酋|卡塔尔|科威特|巴林|阿曼|也门|土耳其|格鲁吉亚|亚美尼亚|阿塞拜疆|俄罗斯|俄远东|乌克兰|白俄罗斯|波兰|芬兰|瑞典|挪威|丹麦|英国|爱尔兰|法国|德国|荷兰|比利时|瑞士|奥地利|捷克|匈牙利|罗马尼亚|塞尔维亚|希腊|意大利|西班牙|葡萄牙|美国|加拿大|墨西哥|古巴|哥伦比亚|委内瑞拉|秘鲁|巴西|智利|阿根廷|澳大利亚|新西兰|斐济|埃及|利比亚|苏丹|埃塞俄比亚|肯尼亚|南非|尼日利亚|津巴布韦|赞比亚|莫桑比克|马达加斯加|地中海|红海|波斯湾|黑海|波罗的海|太平洋|大西洋|印度洋|阿拉斯加|夏威夷|鹿特丹|柏林|慕尼黑|巴黎|伦敦|纽约|洛杉矶|东京|大阪|名古屋|首尔|曼谷|河内|仰光|雅加达|马尼拉|吉隆坡|新德里|孟买|加德满都|达卡|喀布尔|德黑兰|巴格达|迪拜|伊斯坦布尔|开罗|内罗毕|莫斯科|圣彼得堡|基辅|华沙|维也纳|罗马|米兰|马德里|雅典)/;
/* 边境例外：境外灾害波及我国边境的事件仍算国内 */
const CHINA_BORDER_EV_RE = /(吉隆|西藏|日喀则|樟木|普兰|亚东|霍尔果斯|瑞丽|磨憨|凭祥|东兴|丹东|绥芬河|黑河|满洲里|二连浩特)/;

/* 事件类型词（必须是"一件事故/灾害"才会推） */
const EV_TYPE_RE2 = /(火灾|起火|燃爆|爆炸|泥石流|土石流|山体滑坡|滑坡|崩塌|塌方|坍塌|倒塌|地面塌陷|地震|海啸|溃坝|矿难|透水|冒顶|沉船|翻船|倾覆|侧翻|踩踏|坠机|空难|山洪|洪涝|洪水|台风|龙卷风)/i;

/* 重大程度判定：满足其一即"重大" */
const NUM_DEATH_RE = [/(\d+)\s*人?(?:不幸)?遇难/.source, /(?:死亡|罹难)\s*(\d+)\s*人/.source].map(s => new RegExp(s));
const NUM_MISSING_RE = [/(\d+)\s*人?(?:仍然)?失联/.source, /失联\s*(\d+)\s*人/.source].map(s => new RegExp(s));
const MAJOR_WORD_RE = /(特别重大|重大(事故|灾害|火灾|爆炸|交通事故|生产安全事故)|较大事故|Ⅰ级响应|Ⅱ级响应|国家防总|国务院(工作组|调查组|安委会)|国家消防救援局|应急管理部(工作组|启动)|习近平|李强|批示|重要指示)/i;
const CASUALTY_WORD_RE = /(遇难|失联|失踪|死亡|罹难|伤亡|被困|牺牲|殉职|受伤|重伤)/i;

/* 评论/非事件类排除 */
const COMMENT_RE = /(视频｜|视频\||评论|警示|启示|盘点|解读|综述|一周|回眸|回顾|观察|思考|反思|探访|追问|之问|如何看|为何|说明了什么)/i;
/* 例行天气预报 */
const ROUTINE_RE = /(天气预报|天气趋势|未来三天|未来几日|未来十天|蓝色预警|黄色预警|橙色预警|红色预警|发布预警|预警发布|预计.{0,6}(有|出现)|气温)/i;

/* 官方媒体白名单（来源域名 + 媒体名双判） */
const OFFICIAL_DOMAINS = /(cctv\.com|cntv\.cn|news\.cn|xinhuanet\.com|people\.com\.cn|gov\.cn|chinanews\.com\.cn|gmw\.cn|mem\.gov\.cn|cneb\.gov\.cn|china\.com\.cn|cnr\.cn|legaldaily\.com\.cn|chinawater\.com\.cn|cma\.gov\.cn|cea\.gov\.cn|xhby\.net|yicai\.com$)/i;
const OFFICIAL_NAME_RE = /(央视|新华|人民网|人民日报|中国政府网|中国新闻网|中新网|光明|应急管理部|央广|经济日报|法治日报|环球时报|中国应急管理|央视新闻|新华社)/i;

function extractNum(title, res) {
  for (const re of res) {
    const m = title.match(re);
    if (m) return parseInt(m[1], 10);
  }
  return 0;
}

function pickDeaths(title) {
  let d = extractNum(title, NUM_DEATH_RE);
  if (!d) {
    const m2 = title.match(/遇难[^0-9]{0,6}(\d+)/);
    if (m2) d = parseInt(m2[1], 10);
  }
  return d || 0;
}

/* 重大程度判定（用户规则）：
 * ① 伤亡案件：死亡≥2人 或 失联≥3人
 * ② 重大生产安全事故/重大事故/Ⅰ·Ⅱ级响应/国家层面响应/中央领导批示 —— 不论伤亡
 * ③ 重大自然灾害（泥石流/滑坡/地震/山洪/洪涝/台风/海啸/溃坝等）：前期可能只有事件信息
 *    无伤亡消息，也直接推送 */
const GEO_DISASTER_RE = /(泥石流|土石流|山体滑坡|滑坡|地震|海啸|溃坝|山洪|龙卷风)/;
/* 台风/洪涝类无伤亡时需伴随实际影响词（防"台风生成"类例行消息刷屏） */
const GEO_IMPACT_RE = /(登陆|过境|转移|撤离|安置|应急响应|停产|停课|停运|停工|避险)/;

function isMajor(title) {
  const deaths = pickDeaths(title);
  const missing = extractNum(title, NUM_MISSING_RE);
  if (deaths >= 2 || missing >= 3) return { major: true, deaths, missing, why: '人身伤亡' };
  if (MAJOR_WORD_RE.test(title)) return { major: true, deaths, missing, why: '重大事故/批示/响应' };
  if (GEO_DISASTER_RE.test(title)) return { major: true, deaths, missing, why: '重大自然灾害' };
  if (/(台风|洪涝|洪水)/.test(title) && GEO_IMPACT_RE.test(title)) return { major: true, deaths, missing, why: '重大自然灾害' };
  return { major: false, deaths, missing };
}

/* 标题相似分组键（跨媒体同事件标题前缀通常一致） */
function titleKey(t) {
  return t.replace(/[\s\p{P}\p{S}]+/gu, '').replace(/（[^）]*）|\([^)]*\)/g, '').slice(0, 22);
}

function hash(text) {
  return createHash('sha1').update(text).digest('hex').slice(0, 12);
}

async function fetchRss(src) {
  const res = await fetch(src.url, {
    headers: { 'User-Agent': 'Mozilla/5.0 (compatible; IncidentMonitor/1.0)' },
    signal: AbortSignal.timeout(20000),
  });
  if (!res.ok) throw new Error(`HTTP ${res.status}`);
  const xml = await res.text();
  const items = [];
  for (const b of (xml.match(/<item>([\s\S]*?)<\/item>/g) || [])) {
    const title = (b.match(/<title>(?:<!\[CDATA\[)?([\s\S]*?)(?:\]\]>)?<\/title>/) || [])[1] || '';
    const pubDate = (b.match(/<pubDate>(?:<!\[CDATA\[)?([\s\S]*?)(?:\]\]>)?<\/pubDate>/) || [])[1] || '';
    const clean = title.replace(/<!\[CDATA\[|\]\]>/g, '').replace(/&amp;/g, '&').replace(/&quot;/g, '"').replace(/&#39;/g, "'").trim();
    if (!clean) continue;
    const siteM = b.match(/<source url="([^"]+)">([^<]+)<\/source>/);
    items.push({
      title: clean,
      pubDate,
      ts: Date.parse(pubDate) || 0,
      site: siteM ? siteM[2].trim() : '',
      siteUrl: siteM ? siteM[1].trim() : '',
    });
  }
  return items;
}

async function sendWecom(md) {
  const res = await fetch(WEBHOOK, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ msgtype: 'markdown', markdown: { content: md } }),
    signal: AbortSignal.timeout(15000),
  });
  const out = await res.json().catch(() => ({}));
  if (out.errcode !== 0) throw new Error('wecom errcode ' + out.errcode + ' ' + (out.errmsg || ''));
}

async function main() {
  // 1. 抓取
  const all = [];
  for (const src of SOURCES) {
    try {
      const items = await fetchRss(src);
      console.log(`[ok] ${src.name}: ${items.length} 条`);
      all.push(...items);
    } catch (e) {
      console.log(`[warn] ${src.name}: ${e.message}`);
    }
    await new Promise(r => setTimeout(r, 1200));
  }

  // 2. 逐条过滤：国内 + 事件类型 + 重大程度 + 非评论 + 时效
  const minTs = Date.now() - FRESH_HOURS * 3600000;
  const candidates = [];
  const seenTitle = new Set();
  for (const it of all) {
    if (seenTitle.has(it.title)) continue;
    seenTitle.add(it.title);
    if (!it.ts || it.ts < minTs) continue;                    // 超过26小时的旧闻不推
    if (COMMENT_RE.test(it.title)) continue;                  // 评论/盘点类
    if (ROUTINE_RE.test(it.title)) continue;                  // 例行预报
    if (FOREIGN_RE.test(it.title) && !CHINA_BORDER_EV_RE.test(it.title)) continue; // 国外事件
    if (!EV_TYPE_RE2.test(it.title) && !GEO_DISASTER_RE.test(it.title)) continue; // 必须是事故/灾害
    const { major, deaths, missing } = isMajor(it.title);
    if (!major) continue;                                     // 重大程度不够
    candidates.push({ ...it, deaths, missing, key: titleKey(it.title), id: 'PT-' + hash(titleKey(it.title)) });
  }
  console.log(`候选重大事件: ${candidates.length} 条`);

  // 3. 多源印证统计（同一 key 出现于几家不同媒体）
  const keySources = new Map();
  for (const c of candidates) {
    if (!keySources.has(c.key)) keySources.set(c.key, new Set());
    if (c.site) keySources.get(c.key).add(c.site);
  }

  // 4. 可信度把关：官方白名单来源 或 多源印证，二者居其一定为可信
  const trusted = candidates.filter(c => {
    const official = (c.siteUrl && OFFICIAL_DOMAINS.test(c.siteUrl)) || OFFICIAL_NAME_RE.test(c.site || '');
    const multi = (keySources.get(c.key)?.size || 0) >= 2;
    c.official = official; c.multi = multi;
    return official || multi;
  });
  console.log(`通过真实性把关: ${trusted.length} 条`);
  for (const c of candidates) {
    const official = (c.siteUrl && OFFICIAL_DOMAINS.test(c.siteUrl)) || OFFICIAL_NAME_RE.test(c.site || '');
    console.log(`[候选] ${official ? '官方源' : '非官方'}|${keySources.get(c.key)?.size || 1}源|${c.deaths}亡${c.missing}失联|${c.title.slice(0, 45)}`);
  }

  // 5. 去重状态
  mkdirSync(dirname(STATE_FILE), { recursive: true });
  let state = { updated: '', pushed: {} };
  try { state = { ...state, ...JSON.parse(readFileSync(STATE_FILE, 'utf8')) }; } catch { /* 首次运行 */ }
  const now = Date.now();
  for (const [id, info] of Object.entries(state.pushed)) {
    if (now - info.ts > STATE_TTL_MS) delete state.pushed[id];
  }

  // 按伤亡降序 + 时间新优先
  trusted.sort((a, b) => (b.deaths + b.missing) - (a.deaths + a.missing) || b.ts - a.ts);

  // 6. 推送（每轮最多 MAX_PUSH 条；同一事件伤亡显著增加时允许重推一次进展）
  let pushed = 0;
  for (const c of trusted) {
    if (pushed >= MAX_PUSH) break;
    const prev = state.pushed[c.id];
    if (prev) {
      const newCas = c.deaths + c.missing;
      if (!(newCas >= (prev.cas || 0) + 3)) continue; // 已推过且伤亡无显著增加
      console.log(`事件进展重推: ${c.title.slice(0, 30)} (${prev.cas || 0} → ${newCas})`);
    }
    const srcLabel = c.site || '新闻媒体';
    const srcNote = c.official && c.multi ? `${srcLabel} 等多家媒体（多源印证）`
      : c.official ? `${srcLabel}（官方媒体）`
      : `${srcLabel} 等 ${keySources.get(c.key).size} 家媒体（多源印证）`;
    const casText = c.deaths ? `${c.deaths}人遇难` : '暂未收到人员伤亡报告，以官方通报为准';
    const casText2 = c.missing ? `、${c.missing}人失联` : '';
    const when = new Date(c.ts).toLocaleString('zh-CN', { timeZone: 'Asia/Shanghai', hour12: false });
    const md = [
      `## 🚨 重大突发事件快报`,
      `**${c.title}**`,
      `伤亡：${casText}${casText2}（据媒体报道，以官方通报为准）`,
      `来源：${srcNote}，可在其官网按标题检索原文`,
      `时间：${when}`,
      `[事件已收录网页端](${BRIEF_URL})`,
    ].join('\n');
    try {
      await sendWecom(md);
      pushed++;
      state.pushed[c.id] = { ts: now, cas: c.deaths + c.missing, title: c.title.slice(0, 60) };
      console.log(`已推送: ${c.title.slice(0, 40)} | ${srcNote}`);
    } catch (e) {
      console.log(`推送失败: ${e.message}`);
    }
    await new Promise(r => setTimeout(r, 2000));
  }
  if (!pushed) console.log('本轮无新增重大事件，不推送');

  // 7. 写状态（无论是否推送都更新，供工作流 commit 去重持久化）
  state.updated = new Date().toISOString();
  writeFileSync(STATE_FILE, JSON.stringify(state, null, 2), 'utf8');
  console.log(`状态已写入，累计已推 ${Object.keys(state.pushed).length} 个事件`);
}

main().catch((e) => {
  console.error('巡检失败:', e);
  process.exit(1);
});
