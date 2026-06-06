// ============================================================
// POLYMARKET → KALSHI SIGNAL BOT (flat single-file version)
// All modules merged — drop index.js + package.json in root
// ============================================================
require('dotenv').config();
const fs      = require('fs');
const path    = require('path');
const cron    = require('node-cron');
const fetch   = require('node-fetch');
const express = require('express');
const Fuse    = require('fuse.js');
const { v4: uuidv4 } = require('uuid');
const Database = require('better-sqlite3');

// ── CONFIG ────────────────────────────────────────────────────
const CONFIG = {
  kalshi: {
    apiKey:    process.env.KALSHI_API_KEY    || '',
    apiSecret: process.env.KALSHI_API_SECRET || '',
    baseUrl:   process.env.KALSHI_BASE_URL   || 'https://trading-api.kalshi.com',
  },
  discord: {
    webhook: process.env.DISCORD_WEBHOOK_URL || '',
  },
  signal: {
    timeWindowMinutes: parseInt(process.env.TIME_WINDOW_MINUTES)   || 15,
    minTraders:        parseInt(process.env.MIN_TRADERS_FOR_SIGNAL) || 2,
    matchThreshold:    parseInt(process.env.KALSHI_MATCH_THRESHOLD) || 70,
    leaderboardSize:   parseInt(process.env.LEADERBOARD_SIZE)       || 150,
  },
  polling: {
    tradesMinutes:    parseInt(process.env.POLL_TRADES_MINUTES)    || 5,
    leaderboardHours: parseInt(process.env.POLL_LEADERBOARD_HOURS) || 6,
  },
  dashboard: {
    port: parseInt(process.env.PORT || process.env.DASHBOARD_PORT) || 3000,
  },
};

// ── LOGGER ────────────────────────────────────────────────────
const LOG_DIR  = path.join(__dirname, 'logs');
const LOG_FILE = path.join(LOG_DIR, 'signals.log');
fs.mkdirSync(LOG_DIR, { recursive: true });

const ICONS = { INFO:'📡',SIGNAL:'🎯',MATCH:'🔗',SKIP:'⏭',ERROR:'❌',WARN:'⚠️',POLL:'🔄',DASH:'📊' };

function log(level, msg, data={}) {
  const ts   = new Date().toISOString();
  const icon = ICONS[level] || '📋';
  const ext  = Object.keys(data).length ? ' ' + JSON.stringify(data) : '';
  const line = `[${ts}] ${icon} [${level}] ${msg}${ext}`;
  console.log(line);
  fs.appendFileSync(LOG_FILE, line + '\n');
}

// ── DATABASE ──────────────────────────────────────────────────
const DB_PATH = path.join(__dirname, 'data', 'bot.db');
fs.mkdirSync(path.dirname(DB_PATH), { recursive: true });

const db = new Database(DB_PATH);
db.pragma('journal_mode = WAL');

db.exec(`
  CREATE TABLE IF NOT EXISTS traders (
    id TEXT PRIMARY KEY, rank INTEGER,
    profit_30d REAL, volume_30d REAL, fetched_at INTEGER
  );
  CREATE TABLE IF NOT EXISTS trades (
    id TEXT PRIMARY KEY, trader_address TEXT,
    market_id TEXT, market_title TEXT,
    outcome TEXT, side TEXT,
    price REAL, size REAL, usd_value REAL, timestamp INTEGER,
    created_at INTEGER DEFAULT (strftime('%s','now'))
  );
  CREATE INDEX IF NOT EXISTS idx_trades_timestamp ON trades(timestamp);
  CREATE INDEX IF NOT EXISTS idx_trades_trader ON trades(trader_address);
  CREATE TABLE IF NOT EXISTS signals (
    id TEXT PRIMARY KEY, market_id TEXT, market_title TEXT,
    outcome TEXT, traders TEXT, trader_count INTEGER,
    avg_price REAL, window_minutes INTEGER, detected_at INTEGER,
    kalshi_ticker TEXT, kalshi_title TEXT,
    kalshi_confidence REAL, kalshi_price REAL,
    status TEXT DEFAULT 'PENDING', skip_reason TEXT
  );
`);

const DB = {
  upsertTrader: db.prepare(`INSERT OR REPLACE INTO traders (id,rank,profit_30d,volume_30d,fetched_at) VALUES (@id,@rank,@profit_30d,@volume_30d,@fetched_at)`),
  getTopTraders: db.prepare(`SELECT * FROM traders ORDER BY rank ASC LIMIT ?`),
  upsertTrade: db.prepare(`INSERT OR IGNORE INTO trades (id,trader_address,market_id,market_title,outcome,side,price,size,usd_value,timestamp) VALUES (@id,@trader_address,@market_id,@market_title,@outcome,@side,@price,@size,@usd_value,@timestamp)`),
  getRecentBuys: db.prepare(`SELECT * FROM trades WHERE side='BUY' AND timestamp>? ORDER BY timestamp DESC`),
  insertSignal: db.prepare(`INSERT OR IGNORE INTO signals (id,market_id,market_title,outcome,traders,trader_count,avg_price,window_minutes,detected_at,status) VALUES (@id,@market_id,@market_title,@outcome,@traders,@trader_count,@avg_price,@window_minutes,@detected_at,@status)`),
  updateSignal: db.prepare(`UPDATE signals SET kalshi_ticker=@kalshi_ticker,kalshi_title=@kalshi_title,kalshi_confidence=@kalshi_confidence,kalshi_price=@kalshi_price,status=@status,skip_reason=@skip_reason WHERE id=@id`),
  getRecentSignals: db.prepare(`SELECT * FROM signals ORDER BY detected_at DESC LIMIT ?`),
  signalExists: db.prepare(`SELECT id FROM signals WHERE market_id=? AND outcome=? AND detected_at>? LIMIT 1`),
  bulkTraders: (list) => { const tx = db.transaction(l => { for(const t of l) DB.upsertTrader.run(t); }); tx(list); },
  bulkTrades:  (list) => { const tx = db.transaction(l => { for(const t of l) DB.upsertTrade.run(t); }); tx(list); },
};

// ── DISCORD ───────────────────────────────────────────────────
async function discord(msg) {
  if(!CONFIG.discord.webhook) return;
  try {
    await fetch(CONFIG.discord.webhook, {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ content: msg.slice(0,1990) }),
    });
  } catch(e) { log('ERROR', `Discord: ${e.message}`); }
}

// ── POLYMARKET ────────────────────────────────────────────────
const GAMMA = 'https://gamma-api.polymarket.com';
const CLOB  = 'https://clob.polymarket.com';
const sleep = ms => new Promise(r => setTimeout(r, ms));

async function fetchLeaderboard() {
  try {
    log('POLL', 'Fetching Polymarket leaderboard...');

    // Try multiple endpoints and response shapes
    const endpoints = [
      // Crypto-specific leaderboard — exactly what you see on polymarket.com/leaderboard with Crypto filter
      `${GAMMA}/leaderboard?window=monthly&limit=${CONFIG.signal.leaderboardSize}&category=crypto`,
      `${GAMMA}/leaderboard?window=monthly&limit=${CONFIG.signal.leaderboardSize}&tag=crypto`,
      `${GAMMA}/leaderboard?window=monthly&limit=${CONFIG.signal.leaderboardSize}`,
      `${CLOB}/profiles/leaderboard?window=monthly&limit=${CONFIG.signal.leaderboardSize}&category=crypto`,
      `${CLOB}/profiles/leaderboard?window=monthly&limit=${CONFIG.signal.leaderboardSize}`,
    ];

    for(const url of endpoints) {
      try {
        const r    = await fetch(url, { headers:{ Accept:'application/json' } });
        if(!r.ok) continue;
        const data = await r.json();

        // Handle any response shape — find the array
        let raw = null;
        if(Array.isArray(data))           raw = data;
        else if(Array.isArray(data?.data)) raw = data.data;
        else if(Array.isArray(data?.results)) raw = data.results;
        else if(Array.isArray(data?.leaderboard)) raw = data.leaderboard;
        else {
          // Try to find any array property
          for(const v of Object.values(data||{})) {
            if(Array.isArray(v) && v.length > 0) { raw = v; break; }
          }
        }

        if(raw && raw.length > 0) {
          log('POLL', `Leaderboard fetched from ${url.split('?')[0]}`);
          return processLeaderboard(raw);
        }
      } catch(e2) { log('WARN', `Endpoint failed: ${e2.message}`); }
    }

    log('WARN', 'All leaderboard endpoints failed — using mock for testing');
    return [];
  } catch(e) {
    log('ERROR', `fetchLeaderboard: ${e.message}`);
    return [];
  }
}

function processLeaderboard(raw) {
  const traders = raw.map((t,i) => ({
    id:         t.proxyWalletAddress || t.address || t.user || '',
    rank:       t.rank || i+1,
    profit_30d: parseFloat(t.pnl || t.profit || 0),
    volume_30d: parseFloat(t.volume || 0),
    fetched_at: Math.floor(Date.now()/1000),
  })).filter(t => t.id);
  DB.bulkTraders(traders);
  log('POLL', `Leaderboard: ${traders.length} traders stored`);
  return traders;
}

async function fetchTraderTrades(address) {
  try {
    const r = await fetch(`${CLOB}/trades?user=${address}&limit=50`);
    if(!r.ok) return [];
    const data   = await r.json();
    const trades = data?.data || data || [];
    // Crypto keywords — only track crypto markets
    const CRYPTO_KEYWORDS = [
      'btc','bitcoin','eth','ethereum','sol','solana','xrp','ripple',
      'doge','dogecoin','bnb','binance','hype','hyperliquid','avax',
      'avalanche','link','chainlink','matic','polygon','ada','cardano',
      'crypto','price','above','below','higher','lower','15 min','15min',
    ];
    const isCrypto = t => {
      const title = (t.title||t.market_title||'').toLowerCase();
      return CRYPTO_KEYWORDS.some(k => title.includes(k));
    };

    return trades
      .filter(isCrypto)
      .map(t => ({
        id:             t.id || `${address}-${t.timestamp}-${Math.random()}`,
        trader_address: address,
        market_id:      t.market || t.marketId || t.condition_id || '',
        market_title:   t.title  || t.market_title || '',
        outcome:        t.outcome_index===0 ? 'YES' : t.outcome_index===1 ? 'NO' : (t.outcome||'YES'),
        side:           (t.side||t.type||'BUY').toUpperCase().includes('BUY') ? 'BUY' : 'SELL',
        price:          parseFloat(t.price||0),
        size:           parseFloat(t.size||t.matched||0),
        usd_value:      parseFloat(t.usdcSize || (parseFloat(t.price||0)*parseFloat(t.size||0))),
        timestamp:      t.timestamp ? Math.floor(new Date(t.timestamp).getTime()/1000) : Math.floor(Date.now()/1000),
      }));
  } catch(e) { return []; }
}

async function pollAllTraders() {
  const traders = DB.getTopTraders.all(CONFIG.signal.leaderboardSize);
  if(!traders.length){ log('WARN','No traders in DB'); return 0; }
  log('POLL', `Polling ${traders.length} traders...`);
  let total = 0;
  for(const t of traders) {
    const trades = await fetchTraderTrades(t.id);
    if(trades.length) { DB.bulkTrades(trades); total += trades.length; }
    await sleep(200);
  }
  log('POLL', `Poll done — ${total} trades stored`);
  return total;
}

// ── SIGNAL DETECTION ──────────────────────────────────────────
function detectSignals() {
  const windowSecs = CONFIG.signal.timeWindowMinutes * 60;
  const cutoff     = Math.floor(Date.now()/1000) - windowSecs;
  const recentBuys = DB.getRecentBuys.all(cutoff);
  if(!recentBuys.length) return [];

  const topTraders = new Set(DB.getTopTraders.all(CONFIG.signal.leaderboardSize).map(t=>t.id));
  const topBuys    = recentBuys.filter(t => topTraders.has(t.trader_address));
  if(!topBuys.length) return [];

  // Group by market_id + outcome
  const byMarket = new Map();
  for(const trade of topBuys) {
    const key = `${trade.market_id}::${trade.outcome}`;
    if(!byMarket.has(key)) byMarket.set(key, []);
    byMarket.get(key).push(trade);
  }

  // Fuzzy cluster by title
  const titled  = topBuys.filter(t => t.market_title && t.market_title.length > 5);
  const fuse    = new Fuse(titled, { keys:['market_title'], threshold:0.3, includeScore:true });
  const seen    = new Set();
  for(const trade of titled) {
    if(seen.has(trade.id)) continue;
    const matches = fuse.search(trade.market_title)
      .filter(r => r.score < 0.3 && r.item.id !== trade.id && r.item.outcome === trade.outcome)
      .map(r => r.item);
    if(matches.length) {
      const key = `title::${trade.market_title}::${trade.outcome}`;
      byMarket.set(key, [trade, ...matches]);
      [trade, ...matches].forEach(m => seen.add(m.id));
    }
  }

  const newSignals = [];
  for(const [, trades] of byMarket) {
    const byTrader = new Map();
    for(const t of trades) if(!byTrader.has(t.trader_address)) byTrader.set(t.trader_address, t);
    const unique = [...byTrader.values()];
    if(unique.length < CONFIG.signal.minTraders) continue;

    const best     = unique.reduce((a,b) => b.usd_value > a.usd_value ? b : a);
    // Dedup: skip if we already signaled this market+outcome in last 4 hours
    const existing = DB.signalExists.get(best.market_id, best.outcome, Math.floor(Date.now()/1000)-14400);
    if(existing) {
      log('SKIP', `Already signaled "${best.market_title?.slice(0,40)}" ${best.outcome} in last 4h`);
      continue;
    }

    // Also skip if title is too similar to a recent signal (catches same market with different ID)
    const recentSigs = DB.getRecentSignals.all(20);
    const titleDup = recentSigs.some(s => {
      if((Math.floor(Date.now()/1000) - s.detected_at) > 14400) return false; // older than 4h
      if(s.outcome !== best.outcome) return false;
      const a = (s.market_title||'').toLowerCase();
      const b = (best.market_title||'').toLowerCase();
      // Check if they share 3+ words
      const aWords = a.split(/\W+/).filter(w=>w.length>3);
      const bWords = b.split(/\W+/).filter(w=>w.length>3);
      const shared = aWords.filter(w=>bWords.includes(w)).length;
      return shared >= 3;
    });
    if(titleDup) {
      log('SKIP', `Similar signal already sent recently for "${best.market_title?.slice(0,40)}"`);
      continue;
    }

    const avgPrice = unique.reduce((s,t)=>s+t.price,0)/unique.length;
    const signal   = {
      id:             uuidv4(),
      market_id:      best.market_id,
      market_title:   best.market_title || best.market_id,
      outcome:        best.outcome,
      traders:        JSON.stringify(unique.map(t=>t.trader_address)),
      trader_count:   unique.length,
      avg_price:      avgPrice,
      window_minutes: CONFIG.signal.timeWindowMinutes,
      detected_at:    Math.floor(Date.now()/1000),
      status:         'PENDING',
    };
    DB.insertSignal.run(signal);
    newSignals.push(signal);
    log('SIGNAL', `"${best.market_title?.slice(0,50)}" ${best.outcome} | ${unique.length} traders | avg:${(avgPrice*100).toFixed(1)}¢`);
  }
  return newSignals;
}

// ── KALSHI ────────────────────────────────────────────────────
function kalshiHeaders() {
  return { 'Content-Type':'application/json', 'Authorization':`Bearer ${CONFIG.kalshi.apiKey}` };
}

const KALSHI_CRYPTO_TICKERS = ['BTC','ETH','SOL','XRP','DOGE','BNB','HYPE','AVAX','LINK','MATIC','ADA'];

async function searchKalshi(query='', limit=100) {
  try {
    const params = new URLSearchParams({ limit:String(limit), status:'open' });
    if(query) params.set('search', query);
    const r = await fetch(`${CONFIG.kalshi.baseUrl}/trade-api/v2/markets?${params}`, { headers:kalshiHeaders() });
    if(!r.ok) return [];
    const d = await r.json();
    const markets = d.markets || [];

    // Only return 15-minute crypto markets
    return markets.filter(m => {
      const title = (m.title||m.subtitle||'').toUpperCase();
      const isCrypto = KALSHI_CRYPTO_TICKERS.some(t => title.includes(t));
      const is15min  = title.includes('15') || (m.ticker||'').includes('15');
      return isCrypto && is15min;
    });
  } catch(e) { return []; }
}

async function fetchAllKalshi15minMarkets() {
  // Fetch all open 15min crypto Kalshi markets upfront
  const all = [];
  for(const coin of KALSHI_CRYPTO_TICKERS) {
    const markets = await searchKalshi(coin, 50);
    all.push(...markets);
    await sleep(100);
  }
  // Deduplicate by ticker
  const seen = new Set();
  return all.filter(m => { if(seen.has(m.ticker)) return false; seen.add(m.ticker); return true; });
}

async function getKalshiMarket(ticker) {
  try {
    const r = await fetch(`${CONFIG.kalshi.baseUrl}/trade-api/v2/markets/${ticker}`, { headers:kalshiHeaders() });
    if(!r.ok) return null;
    const d = await r.json();
    return d.market || null;
  } catch(e) { return null; }
}

function scoreMatch(polyTitle, polyOutcome, km) {
  let score  = 0;
  const kT   = (km.title||'').toLowerCase();
  const kSub = (km.subtitle||'').toLowerCase();
  const pT   = (polyTitle||'').toLowerCase();

  const fuse = new Fuse([{t:kT}], {keys:['t'], threshold:0.4, includeScore:true});
  const res  = fuse.search(pT);
  if(res.length) score += Math.round((1-(res[0].score||1))*40);

  const pWords = pT.split(/\W+/).filter(w=>w.length>3);
  const kWords = (kT+' '+kSub).split(/\W+/).filter(w=>w.length>3);
  const overlap = pWords.filter(w=>kWords.includes(w)).length;
  score += pWords.length ? Math.round((overlap/pWords.length)*20) : 0;

  const CATS = {
    politics: ['election','president','vote','trump','biden','harris','congress'],
    crypto:   ['bitcoin','btc','eth','crypto','sol','solana'],
    sports:   ['nfl','nba','mlb','nhl','super bowl','championship'],
    finance:  ['fed','interest rate','inflation','gdp','recession'],
    ai:       ['openai','gpt','claude','artificial intelligence'],
  };
  let pCat=null, kCat=null;
  for(const [cat,words] of Object.entries(CATS)){
    if(words.some(w=>pT.includes(w)))          pCat=cat;
    if(words.some(w=>kT.includes(w)||kSub.includes(w))) kCat=cat;
  }
  if(pCat&&kCat&&pCat===kCat) score+=20;
  if(km.yes_bid||km.yes_ask)  score+=20;

  return Math.min(score,100);
}

async function findKalshiMatch(signal) {
  const keywords = (signal.market_title||'')
    .replace(/[^a-zA-Z0-9 ]/g,' ').split(' ')
    .filter(w=>w.length>3).slice(0,3).join(' ');

  if(!keywords) return null;
  log('MATCH', `Searching Kalshi 15min crypto: "${keywords}"`);

  const markets = await searchKalshi(keywords);
  if(!markets.length) {
    log('SKIP', `No 15min crypto Kalshi markets for: "${keywords}"`);
    return null;
  }

  const scored = markets
    .map(m => ({ market:m, confidence:scoreMatch(signal.market_title, signal.outcome, m) }))
    .sort((a,b) => b.confidence-a.confidence);

  const best = scored[0];
  log('MATCH', `Best: "${best.market.title?.slice(0,50)}" | ${best.confidence}/100 | ${best.market.ticker}`);

  if(best.confidence < CONFIG.signal.matchThreshold) {
    log('SKIP', `Confidence ${best.confidence} < ${CONFIG.signal.matchThreshold}`);
    return null;
  }

  const detail   = await getKalshiMarket(best.market.ticker);
  const yesPrice = detail?.yes_ask || detail?.last_price || 0;

  return { ticker:best.market.ticker, title:best.market.title, confidence:best.confidence, yesPrice, noPrice:detail?.no_ask||(1-yesPrice) };
}

// ── ALERT ─────────────────────────────────────────────────────
async function alertSignal(signal, match) {
  const traders = JSON.parse(signal.traders||'[]');
  const avgPct  = (signal.avg_price*100).toFixed(1);

  const msg = [
    `🎯  **SIGNAL DETECTED**`,
    `━━━━━━━━━━━━━━━━━━━━`,
    `📊  **Polymarket:** ${signal.market_title?.slice(0,60)}`,
    `📈  **Outcome:** ${signal.outcome} | Avg: **${avgPct}¢**`,
    `👥  **${traders.length}** top-50 traders agreed`,
    `⏱   Window: **${signal.window_minutes} min**`,
    `━━━━━━━━━━━━━━━━━━━━`,
    match
      ? [`🔗  **Kalshi:** ${match.title?.slice(0,60)}`,
         `🎯  Confidence: **${match.confidence}/100**`,
         `💰  YES price: **${(match.yesPrice*100).toFixed(1)}¢**`,
         `📋  Ticker: \`${match.ticker}\``].join('\n')
      : `⏭  No Kalshi match (threshold: ${CONFIG.signal.matchThreshold}/100)`,
    `━━━━━━━━━━━━━━━━━━━━`,
    `🆔  \`${signal.id.slice(0,8)}\``,
  ].join('\n');

  await discord(msg);
  log('SIGNAL', `Alert sent: ${signal.id.slice(0,8)}`);
}

// ── PROCESS SIGNAL ────────────────────────────────────────────
// Track signals currently being processed to avoid duplicate alerts
const processingSignals = new Set();

async function processSignal(signal) {
  // Skip if same market+outcome is already being processed right now
  const key = `${signal.market_id}::${signal.outcome}`;
  if(processingSignals.has(key)) {
    log('SKIP', `Already processing ${key} — skipping duplicate`);
    return;
  }
  processingSignals.add(key);

  log('SIGNAL', `Processing: "${signal.market_title?.slice(0,50)}" | ${signal.outcome} | ${signal.trader_count} traders`);

  let match = null;
  if(CONFIG.kalshi.apiKey) {
    match = await findKalshiMatch(signal);
    DB.updateSignal.run({
      id: signal.id,
      kalshi_ticker:     match?.ticker     || null,
      kalshi_title:      match?.title      || null,
      kalshi_confidence: match?.confidence || null,
      kalshi_price:      match?.yesPrice   || null,
      status:            match ? 'MATCHED' : 'SKIPPED',
      skip_reason:       match ? null : 'no_match',
    });
  }

  await alertSignal(signal, match);

  DB.updateSignal.run({
    id: signal.id,
    kalshi_ticker:     match?.ticker     || null,
    kalshi_title:      match?.title      || null,
    kalshi_confidence: match?.confidence || null,
    kalshi_price:      match?.yesPrice   || null,
    status:            'ALERTED',
    skip_reason:       null,
  });

  // Release lock
  processingSignals.delete(`${signal.market_id}::${signal.outcome}`);
}

// ── POLL CYCLE ────────────────────────────────────────────────
async function runPollCycle() {
  try {
    await pollAllTraders();
    const newSignals = detectSignals();
    log('POLL', `${newSignals.length} new signals`);
    for(const s of newSignals) await processSignal(s);
  } catch(e) { log('ERROR', `Poll cycle: ${e.message}`); }
}

// ── DASHBOARD ─────────────────────────────────────────────────
function startDashboard() {
  const app      = express();
  const signals  = () => DB.getRecentSignals.all(20);
  const traders  = () => DB.getTopTraders.all(10);

  app.get('/api/signals', (req,res) => res.json(DB.getRecentSignals.all(50)));
  app.get('/api/traders', (req,res) => res.json(DB.getTopTraders.all(50)));

  app.get('/', (req,res) => {
    const sigs  = signals();
    const trads = traders();

    const sigRows = sigs.map(s=>`
      <tr>
        <td>${new Date(s.detected_at*1000).toLocaleTimeString()}</td>
        <td>${(s.market_title||'').slice(0,50)}</td>
        <td><b>${s.outcome}</b></td>
        <td>${s.trader_count}</td>
        <td>${((s.avg_price||0)*100).toFixed(1)}¢</td>
        <td>${s.kalshi_ticker||'—'}</td>
        <td>${s.kalshi_confidence?s.kalshi_confidence.toFixed(0)+'/100':'—'}</td>
        <td>${s.kalshi_price?((s.kalshi_price)*100).toFixed(1)+'¢':'—'}</td>
        <td><span class="b ${s.status}">${s.status}</span></td>
      </tr>`).join('');

    const tradRows = trads.map(t=>`
      <tr>
        <td>#${t.rank}</td>
        <td><code>${(t.id||'').slice(0,20)}...</code></td>
        <td>$${(t.profit_30d||0).toFixed(0)}</td>
        <td>$${(t.volume_30d||0).toFixed(0)}</td>
      </tr>`).join('');

    res.send(`<!DOCTYPE html><html><head><title>Kalshi Signal Bot</title>
<meta http-equiv="refresh" content="30">
<style>
*{box-sizing:border-box;margin:0;padding:0}
body{font-family:'Segoe UI',sans-serif;background:#0f1117;color:#e2e8f0;padding:20px}
h1{color:#a78bfa;margin-bottom:4px;font-size:1.4rem}
h2{color:#7c3aed;font-size:1rem;margin:20px 0 8px;text-transform:uppercase;letter-spacing:1px}
.meta{color:#64748b;font-size:.8rem;margin-bottom:20px}
.cards{display:flex;gap:12px;flex-wrap:wrap;margin-bottom:20px}
.card{background:#1e1e2e;border:1px solid #2d2d3d;border-radius:8px;padding:16px 20px;min-width:140px}
.card .val{font-size:1.6rem;font-weight:bold;color:#a78bfa}
.card .label{font-size:.75rem;color:#64748b;margin-top:2px}
table{width:100%;border-collapse:collapse;margin-bottom:24px;font-size:.82rem}
th{background:#1a1a2e;color:#7c3aed;text-align:left;padding:8px 10px;border-bottom:1px solid #2d2d3d}
td{padding:7px 10px;border-bottom:1px solid #1a1a2e}
tr:hover td{background:#1e1e2e}
.b{padding:2px 8px;border-radius:12px;font-size:.7rem;font-weight:bold}
.b.PENDING{background:#374151;color:#9ca3af}
.b.MATCHED{background:#1e3a5f;color:#60a5fa}
.b.ALERTED{background:#3b1f6b;color:#c4b5fd}
.b.SKIPPED{background:#2d1b1b;color:#ef4444}
code{font-size:.78rem;color:#94a3b8}
</style></head><body>
<h1>📊 Polymarket → Kalshi Signal Bot</h1>
<p class="meta">Auto-refreshes every 30s · ${new Date().toLocaleString()}</p>
<div class="cards">
  <div class="card"><div class="val">${sigs.length}</div><div class="label">Recent Signals</div></div>
  <div class="card"><div class="val">${sigs.filter(s=>s.status==='ALERTED').length}</div><div class="label">Alerted</div></div>
  <div class="card"><div class="val">${sigs.filter(s=>s.kalshi_ticker).length}</div><div class="label">Kalshi Matched</div></div>
  <div class="card"><div class="val">${trads.length}/150</div><div class="label">Traders Tracked</div></div>
</div>
<h2>🎯 Recent Signals</h2>
<table><tr><th>Time</th><th>Market</th><th>Side</th><th>Traders</th><th>Avg Price</th><th>Kalshi</th><th>Confidence</th><th>Price</th><th>Status</th></tr>
${sigRows||'<tr><td colspan="9" style="color:#64748b;text-align:center;padding:20px">No signals yet</td></tr>'}
</table>
<h2>👤 Top Traders</h2>
<table><tr><th>Rank</th><th>Address</th><th>30D Profit</th><th>30D Volume</th></tr>
${tradRows||'<tr><td colspan="4" style="color:#64748b;text-align:center;padding:20px">No traders yet</td></tr>'}
</table>
</body></html>`);
  });

  app.listen(CONFIG.dashboard.port, () =>
    log('DASH', `Dashboard at http://localhost:${CONFIG.dashboard.port}`)
  );
}

// ── MAIN ──────────────────────────────────────────────────────
async function main() {
  console.log('\n╔══════════════════════════════════════════════════════════════╗');
  console.log('║  📊 POLYMARKET → KALSHI SIGNAL BOT                           ║');
  console.log('╚══════════════════════════════════════════════════════════════╝\n');

  log('INFO', `Window:${CONFIG.signal.timeWindowMinutes}min | MinTraders:${CONFIG.signal.minTraders} | Threshold:${CONFIG.signal.matchThreshold}/100 | Crypto 15min only`);
  if(!CONFIG.kalshi.apiKey) log('WARN', 'No Kalshi API key — matching disabled');

  startDashboard();

  const traders = await fetchLeaderboard();
  log('INFO', `Ready: ${traders.length} traders loaded`);

  await discord(
    `📊 **Polymarket → Kalshi Bot ONLINE**\n` +
    `━━━━━━━━━━━━━━━━━━━━\n` +
    `👥 Tracking **${traders.length}** top traders\n` +
    `⏱  Window: **${CONFIG.signal.timeWindowMinutes}min** | Min: **${CONFIG.signal.minTraders}** traders\n` +
    `🔗 Kalshi threshold: **${CONFIG.signal.matchThreshold}/100**\n` +
    `🪙 Crypto 15min markets only | No duplicate signals`
  );

  await runPollCycle();

  // Poll every N minutes
  cron.schedule(`*/${CONFIG.polling.tradesMinutes} * * * *`, () => {
    log('POLL', 'Cron: polling trades');
    runPollCycle();
  });

  // Refresh leaderboard every N hours
  cron.schedule(`0 */${CONFIG.polling.leaderboardHours} * * *`, () => {
    log('POLL', 'Cron: refreshing leaderboard');
    fetchLeaderboard();
  });

  log('INFO', 'Running — Ctrl+C to stop');
}

process.on('SIGINT', async () => {
  log('INFO', 'Shutting down...');
  await discord('🔴 **Polymarket → Kalshi Bot OFFLINE**');
  process.exit(0);
});

process.on('unhandledRejection', e => log('ERROR', `Unhandled: ${e.message}`));
main().catch(e => { log('ERROR', `Fatal: ${e.message}`); process.exit(1); });
