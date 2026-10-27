/**
 * live-logger.mjs — PAPER-TRADING DATA LOGGER (no wallet, no orders, no keys)
 *
 * Every 10 minutes it records, for every open city temperature market:
 *   • the REAL order book of every bucket (top 5 bids + asks with sizes)
 *   • the latest airport reports (METAR) for each market's resolution station
 *
 * Why: the backtests could only see a "price" (often the midpoint of a wide
 * spread), not what you could actually buy or sell at. With this log, every
 * strategy can be re-tested on true asks/bids, including limit orders (option 1).
 *
 *   node live-logger.mjs            (leave it running; Ctrl+C to stop)
 *
 * Output (one file per UTC day, JSON lines, gzip-friendly):
 *   exports/live/books-YYYY-MM-DD.jsonl.gz   (top 5 bids + asks per bucket)
 *   exports/live/metar-YYYY-MM-DD.jsonl.gz
 *   exports/live/markets.json        (bucket ↔ token map, station, close time)
 */
import fs from 'fs';
import axios from 'axios';
import zlib from 'zlib';

const INTERVAL_MIN = 10;
const GAMMA = 'https://gamma-api.polymarket.com';
const CLOB = 'https://clob.polymarket.com';
const AWC = 'https://aviationweather.gov/api/data/metar';
const IEM = 'https://mesonet.agron.iastate.edu/cgi-bin/request/asos.py';
const OUT = 'data';
const MONTHS = ['january', 'february', 'march', 'april', 'may', 'june', 'july', 'august', 'september', 'october', 'november', 'december'];
const CITIES = JSON.parse(fs.readFileSync('cities.json', 'utf8'));

fs.mkdirSync(OUT, { recursive: true });
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const log = (...a) => console.log(new Date().toISOString().slice(11, 19), ...a);
const parseArr = (x) => { if (Array.isArray(x)) return x; try { return JSON.parse(x || '[]'); } catch { return []; } };
// one small file per run (keeps git history small): data/YYYY-MM-DD/books-HHMM.jsonl.gz
const RUN = new Date().toISOString();
const dayFile = (kind) => { const d = `${OUT}/${RUN.slice(0, 10)}`; fs.mkdirSync(d, { recursive: true }); return `${d}/${kind}-${RUN.slice(11, 13)}${RUN.slice(14, 16)}.jsonl.gz`; };
// each append is its own gzip block; the file stays a valid .gz (readable by Python gzip / zcat)
const append = (kind, rows) => { if (rows.length) fs.appendFileSync(dayFile(kind), zlib.gzipSync(rows.map((r) => JSON.stringify(r)).join('\n') + '\n')); };

async function get(url, opts = {}, tries = 4) {
  for (let i = 0; i < tries; i++) {
    try { return (await axios({ url, timeout: 30000, ...opts })).data; }
    catch (e) {
      const st = e.response?.status;
      if (st === 404 || st === 400) return null;
      if (i === tries - 1) throw e;
      await sleep(1500 * (i + 1) + (st === 429 ? 5000 : 0));
    }
  }
}

function stationFromRules(text = '', source = '') {
  // New rules (from ~Oct 25 2026): "…at London City Airport (EGLC)…", source aviationweather.gov ?ids=EGLC
  const awc = String(source).match(/[?&]ids=([A-Z0-9]{4})/i) || text.match(/aviationweather\.gov[^\s"'<>]*?ids=([A-Z0-9]{4})/i);
  if (awc) return awc[1].toUpperCase();
  const paren = text.match(/\(([A-Z]{4})\)/);
  if (paren) return paren[1];
  const wu = text.match(/wunderground\.com\/history\/daily\/(?:[^.\s"'<>]+)\/([a-z0-9]{3,5})/i);
  if (wu) return wu[1].toUpperCase();
  const noaa = text.match(/weather\.gov[^\s"'<>]*?site=([a-z0-9]{3,5})/i);
  if (noaa) return noaa[1].toUpperCase();
  return null;
}

// ─── market discovery (refreshed every 30 min) ───────────────────────────────
let markets = new Map(); // slug -> {buckets:[{outcome, yes, no}], station, closed, end}
let lastDiscover = 0;
async function discover() {
  const now = Date.now();
  const dates = [-1, 0, 1, 2].map((k) => new Date(now + k * 86400e3));
  const slugs = [];
  for (const d of dates) for (const kind of ['highest', 'lowest']) for (const c of CITIES)
    slugs.push(`${kind}-temperature-in-${c}-on-${MONTHS[d.getUTCMonth()]}-${d.getUTCDate()}-${d.getUTCFullYear()}`);
  const next = new Map();
  let i = 0;
  await Promise.all(Array.from({ length: 8 }, async () => {
    while (i < slugs.length) {
      const slug = slugs[i++];
      try {
        const ev = await get(`${GAMMA}/events/slug/${slug}`);
        if (!ev?.markets?.length || ev.closed) continue;
        const buckets = ev.markets.map((m) => {
          const t = parseArr(m.clobTokenIds);
          return { outcome: m.groupItemTitle || m.question, yes: t[0], no: t[1], closed: !!m.closed };
        }).filter((b) => b.yes);
        if (buckets.every((b) => b.closed)) continue;
        next.set(slug, { buckets, station: stationFromRules(ev.description || '', ev.resolutionSource || ''), end: ev.endDate, start: ev.startDate });
      } catch { /* skip */ }
    }
  }));
  if (next.size === 0) {
    // Usually means the internet is down — keep the old list and try again next cycle
    log(`discovery found 0 markets (internet down?) — keeping previous list of ${markets.size}, retrying next cycle`);
    return;
  }
  markets = next;
  lastDiscover = now;
  fs.writeFileSync(`${OUT}/markets.json.gz`, zlib.gzipSync(JSON.stringify({ ts: now, markets: Object.fromEntries(markets) })));
  log(`discovered ${markets.size} open markets`);
}

// ─── order books ─────────────────────────────────────────────────────────────
async function snapBooks() {
  const tokens = [];
  for (const [slug, m] of markets) for (const b of m.buckets) if (!b.closed) tokens.push({ slug, outcome: b.outcome, token: b.yes });
  const byToken = new Map(tokens.map((t) => [t.token, t]));
  const ts = Math.floor(Date.now() / 1000);
  const rows = [];
  for (let k = 0; k < tokens.length; k += 50) {
    const chunk = tokens.slice(k, k + 50);
    const books = await get(`${CLOB}/books`, { method: 'POST', data: chunk.map((c) => ({ token_id: c.token })) }).catch(() => null);
    for (const b of books || []) {
      const meta = byToken.get(b.asset_id);
      if (!meta) continue;
      const bids = b.bids.map((x) => [Number(x.price), Number(x.size)]).sort((a, c) => c[0] - a[0]).slice(0, 5);
      const asks = b.asks.map((x) => [Number(x.price), Number(x.size)]).sort((a, c) => a[0] - c[0]).slice(0, 5);
      rows.push({ ts, slug: meta.slug, bucket: meta.outcome, bids, asks, last: Number(b.last_trade_price) });
    }
    await sleep(200);
  }
  append('books', rows);
  return rows.length;
}

// ─── METAR ───────────────────────────────────────────────────────────────────
const seenMetar = new Set();
async function snapMetar() {
  const ids = [...new Set([...markets.values()].map((m) => m.station).filter(Boolean))];
  const rows = [];
  for (let k = 0; k < ids.length; k += 40) {
    const chunk = ids.slice(k, k + 40);
    let data = await get(AWC, { params: { ids: chunk.join(','), hours: 1, format: 'json' }, headers: { 'User-Agent': 'polyscalp-logger' } })
      .catch((e) => { awcErr = e.message; return null; });
    if (Array.isArray(data)) {
      for (const r of data) {
        const key = `${r.icaoId}|${r.obsTime}`;
        if (seenMetar.has(key)) continue;
        seenMetar.add(key);
        rows.push({ station: r.icaoId, obs: r.obsTime, temp_c: r.temp, raw: r.rawOb, src: 'awc' });
      }
    } else if (data != null) awcErr = `unexpected reply: ${String(JSON.stringify(data)).slice(0, 80)}`;
  }
  // Backup: Iowa Environmental Mesonet (same METAR reports), every 30 min
  if (!rows.length && Date.now() - lastIem > 30 * 60e3) { // backup only when the main feed gave nothing
    lastIem = Date.now();
    const d0 = new Date(Date.now() - 86400e3), d1 = new Date(Date.now() + 86400e3);
    for (const id of ids) {
      const ieId = id.length === 4 && id.startsWith('K') ? id.slice(1) : id;
      const qs = `station=${ieId}&data=tmpf&data=metar&year1=${d0.getUTCFullYear()}&month1=${d0.getUTCMonth() + 1}&day1=${d0.getUTCDate()}` +
        `&year2=${d1.getUTCFullYear()}&month2=${d1.getUTCMonth() + 1}&day2=${d1.getUTCDate()}&tz=Etc/UTC&format=onlycomma&latlon=no&elev=no&missing=M&trace=T&direct=no&report_type=3&report_type=4`;
      const txt = await get(`${IEM}?${qs}`).catch(() => null);
      for (const line of String(txt || '').split('\n').slice(1)) {
        const [, valid, tmpf, raw] = line.split(',');
        if (!valid || tmpf === 'M') continue;
        const key = `${id}|${valid}`;
        if (seenMetar.has(key)) continue;
        seenMetar.add(key);
        rows.push({ station: id, obs: valid, temp_f: Number(tmpf), raw, src: 'iem' });
      }
      await sleep(300);
    }
  }
  append('metar', rows);
  if (!rows.length && awcErr) log('airport feed problem:', awcErr);
  return rows.length;
}
let lastIem = 0, awcErr = null;

async function online() {
  try { await axios.get(`${CLOB}/time`, { timeout: 15000 }); return true; } catch { return false; }
}

async function cycle() {
  if (!(await online())) { log('NO INTERNET — skipping this cycle (check WiFi / router)'); return; }
  try {
    if (Date.now() - lastDiscover > 120 * 60e3) await discover(); // refresh market list every 2h
    const [nb, nm] = await Promise.all([snapBooks(), snapMetar()]);
    log(`books: ${nb} buckets | new airport reports: ${nm}`);
  } catch (e) {
    log('cycle error:', e.message);
  }
}

log(`live-logger (single run) writing to ${OUT}/`);
// GitHub Actions runs this every 10 min: load cached market list, do ONE cycle, exit.
try {
  const c = JSON.parse(zlib.gunzipSync(fs.readFileSync(`${OUT}/markets.json.gz`)).toString());
  if (c.ts && c.markets) { markets = new Map(Object.entries(c.markets)); lastDiscover = c.ts; }
} catch { /* first run */ }
await cycle();
process.exit(0);
