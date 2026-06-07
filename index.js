// ============================================================
// POLYMARKET → KALSHI SIGNAL BOT v4
// - Finds top Polymarket crypto traders via their public API
// - Monitors their trades on 5-min crypto markets
// - When top traders agree → copy on Kalshi 15-min market
// - Auth signing matches the working Python bot exactly
// ============================================================
require('dotenv').config();
const fs        = require('fs');
const path      = require('path');
const fetch     = require('node-fetch');
const WebSocket = require('ws');
const Database  = require('better-sqlite3');
const crypto    = require('crypto');

// ── CONFIG ────────────────────────────────────────────────────
const CONFIG = {
  // Kalshi — use KALSHI_ACCESS_KEY to match old bot's env var name
  // but also accept KALSHI_API_KEY for backwards compat
  kalshiKey:    process.env.KALSHI_ACCESS_KEY    || process.env.KALSHI_API_KEY     || '',
  kalshiSecret: process.env.KALSHI_PRIVATE_KEY_PEM || process.env.KALSHI_PRIVATE_KEY || '',
  kalshiBase:   process.env.KALSHI_BASE_URL      || 'https://api.elections.kalshi.com',

  discord: process.env.DISCORD_WEBHOOK_URL || '',

  ORDER_SIZE_USD:   parseFloat(process.env.ORDER_SIZE_USD)   || 5,
  MIN_WIN_RATE:     parseFloat(process.env.MIN_WIN_RATE)      || 0.55,
  MIN_TRADES_REQ:   parseInt(process.env.MIN_TRADES_REQ)      || 3,
  MIN_AGREE:        parseInt(process.env.MIN_AGREE)            || 1, // 1 top trader = copy
  WINDOW_SECS:      parseInt(process.env.WINDOW_SECS)          || 300,

  SERIES: {
    BTC:  'KXBTC15M',
    ETH:  'KXETH15M',
    SOL:  'KXSOL15M',
    XRP:  'KXXRP15M',
    DOGE: 'KXDOGE15M',
  },

  // Polymarket public endpoints — no auth needed
  POLY_GAMMA:  'https://gamma-api.polymarket.com',
  POLY_CLOB:   'https://clob.polymarket.com',

  // Min Kalshi window — last 5 of every 15 min
  KALSHI_ALERT_OFFSET_SECS: 10 * 60, // fire at 10:00 into each 15-min cycle
};

// ── TOP POLYMARKET TRADERS (hardcoded + refreshed) ────────────
// Known high-profit crypto traders from predicts.guru
// Bot will also discover new ones dynamically
const KNOWN_TRADERS = new Set([
  '0xe9076a87c5ed90ef16e6fe6529c943baeca0cff6',
  '0xa7a8c1fd4bfff08ea30214efa7efaf75d7c6580c',
  '0xb687f00464e33934f5d591f224e71c3559ecaee5',
  '0xbddf61af533ff524d27154e589d2d7a81510c684',
  '0x08fff5b9a79576a7c6e18a9d05ece0658a34ba79',
  '0xdf17f4a8dd01a4cfa6fc3da323a2baee5f8697d1',
  '0x6480542954b70a674a74bd1a6015dec362dc8dc5',
  '0xfe787d2da716d60e8acff57fb87eb13cd4d10319',
  '0x59aed45d6b8c0a4fc67af69a371007b3cceb22d5',
  '0x204f72f35326db932158cba6adff0b9a1da95e14',
  '0x2c335066fe58fe9237c3d3dc7b275c2a034a0563',
  '0x5bb0de4e97698184ead80c80cb17a26cd6f6814b',
  '0x55eca3687ea7d69632ffe0f297ea3d5158bb8c7d',
  '0x84cfffc3f16dcc353094de30d4a45226eccd2f63',
  '0x5d189e816b4149be00977c1a3c8840374aec4972',
  '0x2005d16a84ceefa912d4e380cd32e7ff827875ea',
  '0x9501ec3b8b3e330ae593ebe5b071c3d11b648223',
  '0xa5ea13a81d2b7e8e424b182bdc1db08e756bd96a',
  '0x1521b47bf0c41f6b7fd3ad41cdec566812c8f23e',
  '0x02e7f29f3e612a95a9ccca7131ce7dd5d56b59e5',
  '0xee3ecc39c41e8a6b5399b1cd1b03d72f5271ebb5',
  '0x5268527977f700f9bf9b6d5cd843859e4e70135d',
  '0xfedc381bf3fb5d20433bb4a0216b15dbbc5c6398',
  '0xed107a85a4585a381e48c7f7ca4144909e7dd2e5',
  '0x4099db7f2d394449ccf89c8d42260ecaf1d79fb8',
  '0xc8ab97a9089a9ff7e6ef0688e6e591a066946418',
  '0xeebde7a0e019a63e6b476eb425505b7b3e6eba30',
  '0xb27bc932bf8110d8f78e55da7d5f0497a18b5b82',
  '0x9097b9fd27dd69aa8170e1b16f1b8b839ad70ef0',
  '0xf284ad6d607f777f34bc643cea587c33a886b9f9',
  '0x4f29e103339919c4baaea2a60195cf1c8bb27a7e',
  '0x2663daca3cecf3767ca1c3b126002a8578a8ed1f',
  '0xf8831548531d56ad6a4331493243c447a827cd1f',
  '0xcbba64cddd05171925ffd05d8f8abd38c83fdbff',
  '0x06dc51826bc524d9a83770e7de9dd7e005b04524',
  '0xfea31bc088000ff909be1dfd8d0e3f2c7ef2d227',
  '0xc21ea96be762bb55041529af6e386e7c53b80215',
  '0x45bc74efa620b45c02308acaecdff1f7c06f978b',
  '0xa8b202e6e9a4c2091b6860f1f5c9e9119bbc9a39',
  '0x43e98f912cd6ddadaad88d3297e78c0648e688e5',
  '0x99f0d31fdced5b3a0e5ee2867730a6644a6c9495',
  '0x482bf5accdecfaffa67c14d4d4fbb59f428a3266',
  '0xf5198df69e13937a40d1c76d6f72d9aa067d906b',
  '0xa61ef8773ec2e821962306ca87d4b57e39ff0abd',
  '0x1136368d7f6728e94ed14c532ab95a932f710c2e',
  '0xf4145f880b6e3b099cc7b457e5ef3dbfeb192cd9',
  '0x18469a63386b393ae4bbc6b74621ffe36b81a932',
  '0x70d94a4ff67ed919a8480885cf0808afefe7a684',
  '0xb00a5f0e2718c3ba1c502a55894db64688b477f1',
  '0x672f13d830d3617efea21c2ec7f4bda5d2c27fcc',
]);

// Add extra from env var
if(process.env.TRADER_ADDRESSES) {
  process.env.TRADER_ADDRESSES.split(',').map(a=>a.trim().toLowerCase()).filter(Boolean).forEach(a=>KNOWN_TRADERS.add(a));
}

// ── LOGGER ────────────────────────────────────────────────────
fs.mkdirSync(path.join(__dirname,'logs'), {recursive:true});
const logFile = path.join(__dirname,'logs','bot.log');
function log(level, msg) {
  const line = `[${new Date().toISOString()}] [${level}] ${msg}`;
  console.log(line);
  try { fs.appendFileSync(logFile, line+'\n'); } catch(e){}
}

// ── DATABASE ──────────────────────────────────────────────────
fs.mkdirSync(path.join(__dirname,'data'), {recursive:true});
const db = new Database(path.join(__dirname,'data','bot.db'));
db.pragma('journal_mode = WAL');
db.exec(`
  CREATE TABLE IF NOT EXISTS signals (
    id TEXT PRIMARY KEY, coin TEXT, side TEXT,
    traders TEXT, fired_at INTEGER, kalshi_ticker TEXT,
    entry_price INTEGER, order_id TEXT, status TEXT DEFAULT 'PENDING'
  );
  CREATE TABLE IF NOT EXISTS poly_activity (
    id TEXT PRIMARY KEY, trader TEXT, coin TEXT,
    side TEXT, price REAL, ts INTEGER
  );
`);
const insertSignal   = db.prepare(`INSERT OR IGNORE INTO signals VALUES (@id,@coin,@side,@traders,@fired_at,@kalshi_ticker,@entry_price,@order_id,@status)`);
const insertActivity = db.prepare(`INSERT OR IGNORE INTO poly_activity VALUES (@id,@trader,@coin,@side,@price,@ts)`);
const updateSignal   = db.prepare(`UPDATE signals SET order_id=@order_id, status=@status WHERE id=@id`);

// ── DISCORD ───────────────────────────────────────────────────
async function discord(msg) {
  if(!CONFIG.discord) return;
  try {
    await fetch(CONFIG.discord, {
      method:'POST', headers:{'Content-Type':'application/json'},
      body: JSON.stringify({content: msg.slice(0,1990)}),
    });
  } catch(e){}
}

// ── KALSHI AUTH (matches working Python bot exactly) ──────────
function buildKalshiHeaders(method, urlPath) {
  const ts  = Date.now().toString();
  const key = CONFIG.kalshiSecret;
  const id  = CONFIG.kalshiKey;
  if(!key || !id) {
    log('WARN', 'No Kalshi credentials — cannot place orders');
    return { 'Content-Type':'application/json' };
  }
  try {
    // Rebuild PEM with proper newlines (Railway strips them)
    const BEGIN = '-----BEGIN RSA PRIVATE KEY-----';
    const END   = '-----END RSA PRIVATE KEY-----';
    let b64 = key.trim().replace(BEGIN,'').replace(END,'').replace(/\s+/g,'');
    const wrapped = b64.match(/.{1,64}/g).join('\n');
    const pem = BEGIN + '\n' + wrapped + '\n' + END + '\n';

    // Message = timestamp + METHOD + path (no query string) — exactly like Python bot
    const cleanPath = urlPath.split('?')[0];
    const msg = Buffer.from(ts + method.toUpperCase() + cleanPath);

    const signer = crypto.createSign('SHA256');
    signer.update(msg);
    const sig = signer.sign({
      key:        pem,
      padding:    crypto.constants.RSA_PKCS1_PSS_PADDING,
      saltLength: crypto.constants.RSA_PSS_SALTLEN_DIGEST,
    }, 'base64');

    return {
      'Content-Type':            'application/json',
      'KALSHI-ACCESS-KEY':       id,
      'KALSHI-ACCESS-TIMESTAMP': ts,
      'KALSHI-ACCESS-SIGNATURE': sig,
    };
  } catch(e) {
    log('ERROR', 'Kalshi auth failed: ' + e.message);
    return { 'Content-Type':'application/json' };
  }
}

// ── KALSHI MARKET CACHE ───────────────────────────────────────
const kalshiMarkets = new Map(); // coin → { ticker, yesAsk, yesBid }

async function refreshKalshiMarkets() {
  for(const [coin, series] of Object.entries(CONFIG.SERIES)) {
    try {
      const r = await fetch(`${CONFIG.kalshiBase}/trade-api/v2/markets?series_ticker=${series}&status=open&limit=10`);
      if(!r.ok) continue;
      const d = await r.json();
      const ms = d.markets || [];
      if(!ms.length) continue;

      const now = Math.floor(Date.now()/1000);
      const sorted = ms.map(m => {
        const ct = m.close_time;
        const t  = ct ? (typeof ct==='string' ? Math.floor(new Date(ct).getTime()/1000) : parseInt(ct)>1e10 ? Math.floor(parseInt(ct)/1000) : parseInt(ct)) : 0;
        return { ticker:m.ticker, closeTime:t, yesAsk:m.yes_ask, yesBid:m.yes_bid, lastPrice:m.last_price };
      }).filter(m=>m.closeTime>now-60).sort((a,b)=>a.closeTime-b.closeTime);

      const best = sorted[0] || { ticker:ms[0].ticker, yesAsk:ms[0].yes_ask, yesBid:ms[0].yes_bid };
      kalshiMarkets.set(coin, best);
      log('INFO', `✅ Kalshi ${coin}: ${best.ticker} | ask:${best.yesAsk||'?'}¢`);
    } catch(e) { log('WARN', `refreshKalshiMarkets(${coin}): ${e.message}`); }
  }
  log('INFO', `Kalshi markets: ${kalshiMarkets.size}/5`);
}

// ── KALSHI ORDER ──────────────────────────────────────────────
async function placeKalshiOrder(ticker, side, priceCents, sizeUsd) {
  const orderPath = '/trade-api/v2/portfolio/orders';
  const contracts = Math.max(1, Math.floor(sizeUsd / (priceCents/100)));
  const clientId  = crypto.randomUUID();

  // Order body exactly matches the working Python bot
  const body = {
    ticker,
    client_order_id: clientId,
    action: 'buy',
    side:   side.toLowerCase(),
    count:  contracts,
    type:   'limit',
  };
  if(side.toLowerCase() === 'yes') body.yes_price = Math.round(priceCents);
  else                              body.no_price  = Math.round(priceCents);

  log('INFO', `Placing order: ${ticker} ${side} ${contracts}x @ ${priceCents}¢ (~$${sizeUsd})`);

  try {
    const r = await fetch(`${CONFIG.kalshiBase}${orderPath}`, {
      method:  'POST',
      headers: buildKalshiHeaders('POST', orderPath),
      body:    JSON.stringify(body),
    });
    const d = await r.json();
    if(!r.ok) {
      log('ERROR', `Order failed ${r.status}: ${JSON.stringify(d)}`);
      return null;
    }
    const order = d.order || d;
    log('INFO', `✅ Order placed: ${order.order_id || clientId}`);
    return order;
  } catch(e) {
    log('ERROR', `placeKalshiOrder: ${e.message}`);
    return null;
  }
}

// ── KALSHI WINDOW CHECK ───────────────────────────────────────
function inKalshiWindow() {
  const now    = new Date();
  const modMin = now.getMinutes() % 15;
  const sec    = now.getSeconds();
  return (modMin * 60 + sec) >= CONFIG.KALSHI_ALERT_OFFSET_SECS;
}
function secsToSettlement() {
  const now = new Date();
  const mod = now.getMinutes() % 15;
  const sec = now.getSeconds();
  return (15 - mod) * 60 - sec;
}
function settlementTime() {
  const next = new Date(Date.now() + secsToSettlement()*1000);
  return next.toLocaleTimeString('en-US',{hour:'numeric',minute:'2-digit'});
}

// ── SIGNAL STATE ──────────────────────────────────────────────
// Tracks which traders bought which coin/side within the window
const signalState  = new Map(); // "BTC::YES" → Set of trader addresses
const firedSignals = new Map(); // "BTC::YES::windowId" → true

function windowId() {
  // Changes every 15 min
  return Math.floor(Date.now() / (15*60*1000));
}

async function recordPolyTrade(trader, coin, side, price) {
  const ts  = Math.floor(Date.now()/1000);
  const key = `${coin}::${side}`;
  const wid = windowId();
  const fkey = `${key}::${wid}`;

  // Store activity
  insertActivity.run({ id:`${trader}-${ts}-${Math.random()}`, trader, coin, side, price, ts });

  // Update signal state
  if(!signalState.has(key)) signalState.set(key, new Set());
  signalState.get(key).add(trader);

  const count = signalState.get(key).size;
  log('INFO', `📊 ${coin} ${side}: ${count} top trader(s) this window | latest: ${trader.slice(0,10)}`);

  // Already fired this window?
  if(firedSignals.has(fkey)) {
    log('INFO', `  Already signaled ${key} this window`);
    return;
  }

  // Not in Kalshi window yet?
  if(!inKalshiWindow()) {
    const secsLeft = CONFIG.KALSHI_ALERT_OFFSET_SECS - ((new Date().getMinutes()%15)*60 + new Date().getSeconds());
    log('INFO', `  ${count} trader(s) noted — waiting for Kalshi window (${Math.round(secsLeft/60)}m${secsLeft%60}s)`);
    return;
  }

  // Need at least MIN_AGREE traders
  if(count < CONFIG.MIN_AGREE) return;

  // Fire!
  firedSignals.set(fkey, true);
  await fireSignal(coin, side, [...signalState.get(key)]);
}

async function fireSignal(coin, side, traders) {
  const market = kalshiMarkets.get(coin);
  if(!market) {
    log('WARN', `No Kalshi market for ${coin}`);
    return;
  }

  const secsLeft = secsToSettlement();
  const priceCents = side==='YES'
    ? (market.yesAsk || market.lastPrice || 50)
    : (100 - (market.yesBid || 50));

  if(!priceCents || priceCents < 5 || priceCents > 95) {
    log('WARN', `Price ${priceCents}¢ out of range for ${coin} ${side}`);
    return;
  }

  const sigId = `${coin}-${side}-${Date.now()}`;
  log('INFO', `🎯 SIGNAL: ${coin} ${side} | ${traders.length} trader(s) | ${secsLeft}s to ${settlementTime()}`);

  await discord([
    `🎯 **SIGNAL — ${coin} ${side}**`,
    `━━━━━━━━━━━━━━━━━━━━`,
    `👥 **${traders.length}** top Polymarket trader(s) bought **${coin} ${side}**`,
    `⏱  **${secsLeft}s** until Kalshi settles at **${settlementTime()}**`,
    `🎯 Market: **${market.ticker}** | Price: **${priceCents}¢**`,
    `💸 Placing **$${CONFIG.ORDER_SIZE_USD}** order now...`,
  ].join('\n'));

  // Place order
  const order = await placeKalshiOrder(market.ticker, side, priceCents, CONFIG.ORDER_SIZE_USD);

  insertSignal.run({
    id: sigId, coin, side,
    traders: JSON.stringify(traders),
    fired_at: Math.floor(Date.now()/1000),
    kalshi_ticker: market.ticker,
    entry_price: priceCents,
    order_id: order?.order_id || null,
    status: order ? 'PLACED' : 'FAILED',
  });

  if(order) {
    await discord([
      `✅ **ORDER PLACED — ${coin} ${side}**`,
      `🎯 **${market.ticker}** | **${priceCents}¢** | **$${CONFIG.ORDER_SIZE_USD}**`,
      `🆔 \`${order.order_id || 'submitted'}\``,
      `⏱  Settles in **${secsLeft}s** at **${settlementTime()}**`,
    ].join('\n'));
  } else {
    await discord(`❌ **ORDER FAILED** — ${coin} ${side} — check Railway logs`);
  }
}

// ── POLYMARKET: FIND ACTIVE CRYPTO MARKETS ────────────────────
// Uses Gamma API to find active 5-min markets and their condition IDs
// Then subscribes to their trade feed via Polymarket WebSocket

const polyMarkets = new Map(); // conditionId → { coin, title }
const processedPolyTrades = new Set();

async function findPolyMarkets() {
  const COIN_KEYWORDS = {
    BTC:  ['bitcoin','btc'],
    ETH:  ['ethereum','eth'],
    SOL:  ['solana','sol'],
    XRP:  ['xrp'],
    DOGE: ['doge'],
  };

  // Strategy 1: GET /markets directly — returns array of market objects
  // Each has clobTokenIds[0]=YES, clobTokenIds[1]=NO
  const urls = [
    `${CONFIG.POLY_GAMMA}/markets?active=true&closed=false&limit=500&order=volume24hr&ascending=false`,
    `${CONFIG.POLY_GAMMA}/markets?active=true&closed=false&limit=500`,
    `${CONFIG.POLY_GAMMA}/events?active=true&closed=false&limit=200`,
  ];

  let found = 0;

  for(const url of urls) {
    try {
      log('INFO', `Trying: ${url}`);
      const r = await fetch(url, {
        headers: { 'Accept':'application/json', 'User-Agent':'Mozilla/5.0' }
      });
      if(!r.ok) { log('WARN', `${url} → ${r.status}`); continue; }

      const raw = await r.json();
      log('INFO', `Response type: ${typeof raw} isArray:${Array.isArray(raw)} keys:${Array.isArray(raw)?'N/A':Object.keys(raw||{}).join(',')}`);

      // Handle both array and object responses
      let markets = [];
      if(Array.isArray(raw)) {
        // /markets returns array directly
        markets = raw;
      } else if(Array.isArray(raw.markets)) {
        markets = raw.markets;
      } else if(Array.isArray(raw.data)) {
        // Could be events with nested markets
        for(const ev of raw.data) {
          if(Array.isArray(ev.markets)) markets.push(...ev.markets);
          else markets.push(ev);
        }
      } else if(Array.isArray(raw.events)) {
        for(const ev of raw.events) {
          if(Array.isArray(ev.markets)) markets.push(...ev.markets);
        }
      }

      log('INFO', `Got ${markets.length} market objects to scan`);
      if(markets.length === 0) continue;

      // Log first item structure for debugging
      if(markets[0]) {
        log('INFO', `First market keys: ${Object.keys(markets[0]).join(', ')}`);
        log('INFO', `First market sample: ${JSON.stringify(markets[0]).slice(0,200)}`);
      }

      for(const m of markets) {
        const t = (m.question || m.title || '').toLowerCase();
        if(!t.includes('up or down')) continue;

        let coin = null;
        for(const [c,kw] of Object.entries(COIN_KEYWORDS)) {
          if(kw.some(k=>t.includes(k))){ coin=c; break; }
        }
        if(!coin) continue;

        // clobTokenIds can be array or JSON string
        let clobIds = m.clobTokenIds;
        if(typeof clobIds === 'string') {
          try { clobIds = JSON.parse(clobIds); } catch(e) { clobIds = null; }
        }
        const yesId  = clobIds?.[0];
        const noId   = clobIds?.[1];
        const condId = m.conditionId || m.condition_id || m.id;

        if(!yesId && !condId) continue;

        polyMarkets.set(condId, { coin, title: m.question || m.title, yesId, noId });
        log('INFO', `✅ Poly ${coin}: ${(m.question||'').slice(0,50)} | YES:${String(yesId||'').slice(0,10)}`);
        found++;
      }

      if(found > 0) {
        log('INFO', `Found ${found} Polymarket 5-min crypto markets`);
        return; // success — don't try other URLs
      }
    } catch(e) { log('WARN', `findPolyMarkets(${url}): ${e.message}`); }
  }

  log('WARN', `Found 0 markets — Polymarket API may be returning unexpected format`);
}

// ── POLYMARKET WEBSOCKET ──────────────────────────────────────
// Subscribe to trade events on active 5-min crypto markets
// When a top trader buys → record it and check for signal

let polyWs = null;

function connectPolyWS() {
  const WS_URL = 'wss://ws-subscriptions-clob.polymarket.com/ws/market';
  log('INFO', 'Connecting to Polymarket WebSocket...');
  polyWs = new WebSocket(WS_URL);

  polyWs.on('open', () => {
    log('INFO', '✅ Polymarket WebSocket connected');
    subscribePolyMarkets();
  });

  polyWs.on('message', async (raw) => {
    try {
      const msgs = JSON.parse(raw.toString());
      const list = Array.isArray(msgs) ? msgs : [msgs];
      for(const msg of list) {
        if(msg.event_type === 'trade' || msg.type === 'trade') {
          await handlePolyTrade(msg);
        }
      }
    } catch(e) {}
  });

  polyWs.on('error', e => log('ERROR', `Poly WS: ${e.message}`));
  polyWs.on('close', code => {
    log('INFO', `Poly WS closed (${code}) — reconnect in 5s`);
    setTimeout(connectPolyWS, 5000);
  });

  setInterval(() => {
    if(polyWs?.readyState === WebSocket.OPEN) polyWs.ping();
  }, 20000);
}

function subscribePolyMarkets() {
  if(!polyWs || polyWs.readyState !== WebSocket.OPEN) return;
  const assetIds = [...polyMarkets.values()]
    .flatMap(m => [m.yesId, m.noId].filter(Boolean))
    .slice(0, 200);

  if(!assetIds.length) {
    log('WARN', 'No Polymarket asset IDs yet — will retry after market refresh');
    return;
  }

  polyWs.send(JSON.stringify({ auth:{}, type:'Market', markets:[], assets:assetIds }));
  log('INFO', `Subscribed to ${assetIds.length} Polymarket asset IDs`);
}

async function handlePolyTrade(event) {
  const tradeId = event.id || event.trade_id;
  if(tradeId && processedPolyTrades.has(tradeId)) return;
  if(tradeId) {
    processedPolyTrades.add(tradeId);
    if(processedPolyTrades.size > 10000) processedPolyTrades.clear();
  }

  // Find maker/taker — check if either is a top trader
  const maker = (event.maker_address || event.maker || '').toLowerCase();
  const taker = (event.taker_address || event.taker || '').toLowerCase();
  const trader = KNOWN_TRADERS.has(maker) ? maker : KNOWN_TRADERS.has(taker) ? taker : null;

  if(!trader) return; // not a top trader — ignore

  // Find which market/coin this trade belongs to
  const assetId = event.asset_id;
  let coin = null, side = null;

  for(const [condId, mkt] of polyMarkets) {
    if(mkt.yesId === assetId) { coin = mkt.coin; side = 'YES'; break; }
    if(mkt.noId  === assetId) { coin = mkt.coin; side = 'NO';  break; }
  }
  if(!coin) return;

  const price = parseFloat(event.price || 0);
  log('INFO', `⚡ TOP TRADER ${trader.slice(0,10)}... | ${coin} ${side} @ ${(price*100).toFixed(0)}¢`);
  await recordPolyTrade(trader, coin, side, price);
}

// ── POLYMARKET POLLING FALLBACK ───────────────────────────────
// Since WS may miss trades, also poll the Gamma API for recent activity
// on our tracked markets every 30s outside window, 10s inside

async function pollPolyActivity() {
  // Use Gamma events to find recent position changes
  // by checking open interest / last trade timestamps
  // This is a lightweight check — just see if our known traders
  // have any recent positions in crypto markets

  const cutoff = Math.floor(Date.now()/1000) - CONFIG.WINDOW_SECS;

  for(const trader of [...KNOWN_TRADERS].slice(0, 30)) {
    try {
      // Gamma profiles endpoint shows recent positions
      const r = await fetch(`${CONFIG.POLY_GAMMA}/profiles/${trader}`, {
        headers: { 'Accept':'application/json', 'User-Agent':'Mozilla/5.0' }
      });
      if(!r.ok) continue;
      const d = await r.json();

      // Look at recent positions
      const positions = d.positions || d.openPositions || [];
      for(const pos of positions) {
        const title = (pos.title || pos.market_title || '').toLowerCase();
        if(!title.includes('up or down')) continue;

        let coin = null;
        if(title.includes('bitcoin')||title.includes('btc'))      coin='BTC';
        else if(title.includes('ethereum')||title.includes('eth')) coin='ETH';
        else if(title.includes('solana')||title.includes('sol'))   coin='SOL';
        else if(title.includes('xrp'))                             coin='XRP';
        else if(title.includes('doge'))                            coin='DOGE';
        if(!coin) continue;

        const side  = (pos.outcome||'').toUpperCase().includes('YES') ? 'YES' : 'NO';
        const ts    = pos.lastUpdated ? Math.floor(new Date(pos.lastUpdated).getTime()/1000) : 0;
        if(ts < cutoff) continue;

        const price = parseFloat(pos.currentPrice || pos.price || 0.5);
        const actId = `${trader}-${coin}-${side}-${ts}`;
        const existing = db.prepare('SELECT id FROM poly_activity WHERE id=?').get(actId);
        if(existing) continue;

        log('INFO', `📡 Profile poll: ${trader.slice(0,10)}... has ${coin} ${side} @ ${(price*100).toFixed(0)}¢`);
        await recordPolyTrade(trader, coin, side, price);
      }
    } catch(e) {}
    await new Promise(r=>setTimeout(r,100));
  }
}

// ── WINDOW RESET ──────────────────────────────────────────────
// Clear signal state at the start of each new 15-min window
let lastWindowId = windowId();
function checkWindowReset() {
  const current = windowId();
  if(current !== lastWindowId) {
    lastWindowId = current;
    signalState.clear();
    log('INFO', `🔄 New 15-min window — signal state cleared`);
    // Refresh Kalshi markets for new window
    refreshKalshiMarkets().catch(()=>{});
    // Refresh Poly markets
    findPolyMarkets().then(() => subscribePolyMarkets()).catch(()=>{});
  }
}

// ── MAIN ──────────────────────────────────────────────────────
async function main() {
  console.log('\n╔══════════════════════════════════════════════════════════════╗');
  console.log('║  📊 POLYMARKET → KALSHI COPY BOT v4                          ║');
  console.log('║  Polymarket WS + top trader detection → Kalshi orders        ║');
  console.log('╚══════════════════════════════════════════════════════════════╝\n');

  if(!CONFIG.kalshiKey)    log('WARN', 'No KALSHI_ACCESS_KEY or KALSHI_API_KEY');
  if(!CONFIG.kalshiSecret) log('WARN', 'No KALSHI_PRIVATE_KEY_PEM or KALSHI_PRIVATE_KEY');
  log('INFO', `Watching ${KNOWN_TRADERS.size} top Polymarket traders`);
  log('INFO', `Signal threshold: ${CONFIG.MIN_AGREE}+ traders | Window: last 5min before Kalshi settlement`);
  log('INFO', `Order size: $${CONFIG.ORDER_SIZE_USD}`);

  // Load markets
  await refreshKalshiMarkets();
  await findPolyMarkets();

  // Connect Polymarket WebSocket
  connectPolyWS();

  await discord([
    `📊 **POLYMARKET → KALSHI COPY BOT v4 ONLINE**`,
    `━━━━━━━━━━━━━━━━━━━━`,
    `👥 Watching **${KNOWN_TRADERS.size}** top Polymarket traders`,
    `🎯 Signal fires when **${CONFIG.MIN_AGREE}+** trader(s) buy same side`,
    `⏱  Only trades in **last 5 min** before Kalshi settlement`,
    `💸 **$${CONFIG.ORDER_SIZE_USD}** per trade`,
    `📡 Polymarket WebSocket + polling fallback`,
  ].join('\n'));

  // Poll every 30s outside window, 10s inside
  setInterval(async () => {
    checkWindowReset();
    const inWindow = inKalshiWindow();
    if(inWindow) {
      const secsLeft = secsToSettlement();
      log('INFO', `⏰ IN WINDOW | ${secsLeft}s to ${settlementTime()} | Kalshi:${kalshiMarkets.size} Poly:${polyMarkets.size}`);
      // Log current signal state
      for(const [key, traders] of signalState) {
        if(traders.size > 0) log('INFO', `  ${key}: ${traders.size} trader(s)`);
      }
    }
    await pollPolyActivity();
  }, 10000);

  // Status every 5 min
  setInterval(() => {
    log('INFO', `Status | Kalshi:${kalshiMarkets.size} | Poly markets:${polyMarkets.size} | Traders:${KNOWN_TRADERS.size}`);
  }, 5*60*1000);

  log('INFO', '✅ Running...');
}

process.on('SIGINT', async () => {
  log('INFO', 'Shutting down...');
  await discord('🔴 **Copy Bot OFFLINE**');
  process.exit(0);
});
process.on('unhandledRejection', e => log('ERROR', `Unhandled: ${e.message}`));
main().catch(e => { log('ERROR', `Fatal: ${e.message}`); process.exit(1); });
