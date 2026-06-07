// ============================================================
// POLYMARKET → KALSHI SIGNAL BOT v2
// Watches Polymarket 5-min crypto markets via WebSocket
// Detects when 2+ top-100 traders buy the same side
// Alerts you to copy on Kalshi during the last 5 min window
// ============================================================
require('dotenv').config();
const fs        = require('fs');
const path      = require('path');
const fetch     = require('node-fetch');
const WebSocket = require('ws');
const Database  = require('better-sqlite3');
const crypto    = require('crypto'); // built-in Node.js — no install needed


// ── CONFIG ────────────────────────────────────────────────────
const CONFIG = {
  discord:   process.env.DISCORD_WEBHOOK_URL || '',
  kalshiKey: process.env.KALSHI_API_KEY      || '',
  kalshiUrl: process.env.KALSHI_BASE_URL     || 'https://trading-api.kalshi.com',

  // Signal rules
  minTraders:      parseInt(process.env.MIN_TRADERS)      || 2,   // 2+ traders to signal
  windowSecs:      parseInt(process.env.WINDOW_SECS)      || 300, // 5 min window
  minTradeUsd:     parseFloat(process.env.MIN_TRADE_USD)  || 10,  // ignore tiny trades
  kalshiWindowMin: parseInt(process.env.KALSHI_WINDOW_MIN)|| 5,   // alert in last N min

  // Polymarket
  POLY_WS:   'wss://ws-subscriptions-clob.polymarket.com/ws/market',
  POLY_REST: 'https://gamma-api.polymarket.com',
  CLOB_REST: 'https://clob.polymarket.com',

  // Coins to watch
  COINS: ['BTC','ETH','SOL','XRP','DOGE','BNB','HYPE'],
};

// ── TOP 100 TRADERS (from predicts.guru crypto leaderboard) ──
// These are the actual Polymarket proxy wallet addresses
// of the top traders by volume/profit in crypto markets
const TOP_TRADERS = new Set([
  // From predicts.guru leaderboard - top 50 by volume
  "0xe9076a87c5ed90ef16e6fe6529c943baeca0cff6",
  "0xa7a8c1fd4bfff08ea30214efa7efaf75d7c6580c",
  "0xb687f00464e33934f5d591f224e71c3559ecaee5",
  "0xbddf61af533ff524d27154e589d2d7a81510c684",
  "0x08fff5b9a79576a7c6e18a9d05ece0658a34ba79",
  "0xdf17f4a8dd01a4cfa6fc3da323a2baee5f8697d1",
  "0x6480542954b70a674a74bd1a6015dec362dc8dc5",
  "0xfe787d2da716d60e8acff57fb87eb13cd4d10319",
  "0x59aed45d6b8c0a4fc67af69a371007b3cceb22d5",
  "0x204f72f35326db932158cba6adff0b9a1da95e14",
  "0x2c335066fe58fe9237c3d3dc7b275c2a034a0563",
  "0x5bb0de4e97698184ead80c80cb17a26cd6f6814b",
  "0x55eca3687ea7d69632ffe0f297ea3d5158bb8c7d",
  "0x84cfffc3f16dcc353094de30d4a45226eccd2f63",
  "0x5d189e816b4149be00977c1a3c8840374aec4972",
  "0x2005d16a84ceefa912d4e380cd32e7ff827875ea",
  "0x9501ec3b8b3e330ae593ebe5b071c3d11b648223",
  "0xa5ea13a81d2b7e8e424b182bdc1db08e756bd96a",
  "0x1521b47bf0c41f6b7fd3ad41cdec566812c8f23e",
  "0x02e7f29f3e612a95a9ccca7131ce7dd5d56b59e5",
  "0xee3ecc39c41e8a6b5399b1cd1b03d72f5271ebb5",
  "0x5268527977f700f9bf9b6d5cd843859e4e70135d",
  "0xfedc381bf3fb5d20433bb4a0216b15dbbc5c6398",
  "0xed107a85a4585a381e48c7f7ca4144909e7dd2e5",
  "0x4099db7f2d394449ccf89c8d42260ecaf1d79fb8",
  "0xc8ab97a9089a9ff7e6ef0688e6e591a066946418",
  "0xeebde7a0e019a63e6b476eb425505b7b3e6eba30",
  "0xb27bc932bf8110d8f78e55da7d5f0497a18b5b82",
  "0x9097b9fd27dd69aa8170e1b16f1b8b839ad70ef0",
  "0xf284ad6d607f777f34bc643cea587c33a886b9f9",
  "0x4f29e103339919c4baaea2a60195cf1c8bb27a7e",
  "0x2663daca3cecf3767ca1c3b126002a8578a8ed1f",
  "0xf8831548531d56ad6a4331493243c447a827cd1f",
  "0xcbba64cddd05171925ffd05d8f8abd38c83fdbff",
  "0x06dc51826bc524d9a83770e7de9dd7e005b04524",
  "0xfea31bc088000ff909be1dfd8d0e3f2c7ef2d227",
  "0xc21ea96be762bb55041529af6e386e7c53b80215",
  "0x45bc74efa620b45c02308acaecdff1f7c06f978b",
  "0xa8b202e6e9a4c2091b6860f1f5c9e9119bbc9a39",
  "0x43e98f912cd6ddadaad88d3297e78c0648e688e5",
  "0x99f0d31fdced5b3a0e5ee2867730a6644a6c9495",
  "0x482bf5accdecfaffa67c14d4d4fbb59f428a3266",
  "0xf5198df69e13937a40d1c76d6f72d9aa067d906b",
  "0xa61ef8773ec2e821962306ca87d4b57e39ff0abd",
  "0x1136368d7f6728e94ed14c532ab95a932f710c2e",
  "0xf4145f880b6e3b099cc7b457e5ef3dbfeb192cd9",
  "0x18469a63386b393ae4bbc6b74621ffe36b81a932",
  "0x70d94a4ff67ed919a8480885cf0808afefe7a684",
  "0xb00a5f0e2718c3ba1c502a55894db64688b477f1",
  "0x672f13d830d3617efea21c2ec7f4bda5d2c27fcc",
  // Extra addresses from TRADER_ADDRESSES env var added at runtime
]);

// ── LOGGER ────────────────────────────────────────────────────
fs.mkdirSync(path.join(__dirname,'logs'), { recursive:true });
const logFile = path.join(__dirname,'logs','bot.log');
function log(level, msg) {
  const line = `[${new Date().toISOString()}] [${level}] ${msg}`;
  console.log(line);
  fs.appendFileSync(logFile, line+'\n');
}

// ── DATABASE ──────────────────────────────────────────────────
fs.mkdirSync(path.join(__dirname,'data'), { recursive:true });
const db = new Database(path.join(__dirname,'data','signals.db'));
db.pragma('journal_mode = WAL');
db.exec(`
  CREATE TABLE IF NOT EXISTS signals (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    coin TEXT, side TEXT,
    traders TEXT, trader_count INTEGER,
    kalshi_ticker TEXT, kalshi_price REAL,
    fired_at INTEGER, window_end INTEGER
  );
  CREATE TABLE IF NOT EXISTS trades_seen (
    id TEXT PRIMARY KEY,
    trader TEXT, coin TEXT, side TEXT,
    price REAL, size_usd REAL, ts INTEGER
  );
`);

const insertSignal = db.prepare(`
  INSERT INTO signals (coin,side,traders,trader_count,kalshi_ticker,kalshi_price,fired_at,window_end)
  VALUES (@coin,@side,@traders,@trader_count,@kalshi_ticker,@kalshi_price,@fired_at,@window_end)
`);
const insertTrade = db.prepare(`
  INSERT OR IGNORE INTO trades_seen (id,trader,coin,side,price,size_usd,ts)
  VALUES (@id,@trader,@coin,@side,@price,@size_usd,@ts)
`);

// ── DISCORD ───────────────────────────────────────────────────
async function discord(msg) {
  if(!CONFIG.discord) return;
  try {
    await fetch(CONFIG.discord, {
      method:'POST', headers:{'Content-Type':'application/json'},
      body: JSON.stringify({ content: msg.slice(0,1990) }),
    });
  } catch(e) { log('ERROR',`Discord: ${e.message}`); }
}

// ── SIGNAL STATE ──────────────────────────────────────────────
// key: "BTC::YES" → { traders: Set, firstSeen: ts, lastAlerted: ts }
const signalState = new Map();
const alertedThisWindow = new Set(); // prevent duplicate alerts per window

// ── KALSHI WINDOW DETECTION ───────────────────────────────────
// Returns true if we're in the last N minutes before a :00/:15/:30/:45
function inKalshiAlertWindow() {
  const now     = new Date();
  const minutes = now.getMinutes();
  const secs    = now.getSeconds();
  const mod     = minutes % 15; // 0-14, where 0=settlement, 10-14=alert window
  const totalSecs = mod * 60 + secs;
  const windowStart = (15 - CONFIG.kalshiWindowMin) * 60; // e.g. 10:00 for 5min window
  return totalSecs >= windowStart;
}

// Returns seconds until next Kalshi settlement
function secsToNextSettlement() {
  const now   = new Date();
  const mod   = now.getMinutes() % 15;
  const secs  = now.getSeconds();
  return (15 - mod) * 60 - secs;
}

// Returns the next settlement time string e.g. "1:15 PM"
function nextSettlementTime() {
  const now  = new Date();
  const mod  = now.getMinutes() % 15;
  const next = new Date(now.getTime() + secsToNextSettlement()*1000);
  return next.toLocaleTimeString('en-US',{hour:'numeric',minute:'2-digit'});
}

// ── KALSHI MARKET LOOKUP ──────────────────────────────────────
const kalshiCache = new Map(); // coin → { ticker, price, title }

async function fetchKalshiMarket(coin) {
  if(!CONFIG.kalshiKey) return null;
  try {
    const r = await fetch(
      `${CONFIG.kalshiUrl}/trade-api/v2/markets?status=open&search=${coin}+15&limit=50`,
      { headers:{ 'Authorization':`Bearer ${CONFIG.kalshiKey}`, 'Accept':'application/json' } }
    );
    if(!r.ok) return null;
    const d = await r.json();
    const markets = (d.markets||[]).filter(m => {
      const t = (m.title||'').toUpperCase();
      return t.includes(coin) && (t.includes('15') || t.includes('MIN'));
    });
    if(!markets.length) return null;

    // Pick the one settling soonest
    const best = markets[0];
    const detail = await fetch(
      `${CONFIG.kalshiUrl}/trade-api/v2/markets/${best.ticker}`,
      { headers:{ 'Authorization':`Bearer ${CONFIG.kalshiKey}`, 'Accept':'application/json' } }
    );
    if(!detail.ok) return { ticker: best.ticker, title: best.title, price: null };
    const dd = await detail.json();
    return {
      ticker: best.ticker,
      title:  best.title,
      price:  dd.market?.yes_ask || dd.market?.last_price || null,
    };
  } catch(e) {
    log('ERROR',`Kalshi lookup ${coin}: ${e.message}`);
    return null;
  }
}

// ── KALSHI ORDER PLACEMENT ───────────────────────────────────

function signKalshiRequest(method, path, timestamp, privateKeyPem) {
  // Kalshi requires RSA-PSS SHA256 signature
  // Sign: timestamp + method + path (no query params)
  const message = timestamp + method.toUpperCase() + path;
  try {
    const sign = crypto.createSign('SHA256');
    sign.update(message);
    sign.end();
    // RSA-PSS padding
    const sig = sign.sign({
      key:               privateKeyPem,
      padding:           crypto.constants.RSA_PKCS1_PSS_PADDING,
      saltLength:        crypto.constants.RSA_PSS_SALTLEN_DIGEST,
    });
    return sig.toString('base64');
  } catch(e) {
    log('ERROR', `Kalshi signing failed: ${e.message}`);
    return null;
  }
}

function kalshiAuthHeaders(method, urlPath) {
  const timestamp = Date.now().toString();
  const privateKey = process.env.KALSHI_PRIVATE_KEY || '';
  const apiKeyId   = process.env.KALSHI_API_KEY     || '';

  if(!privateKey || !apiKeyId) return null;

  const sig = signKalshiRequest(method, urlPath, timestamp, privateKey);
  if(!sig) return null;

  return {
    'Content-Type':           'application/json',
    'KALSHI-ACCESS-KEY':      apiKeyId,
    'KALSHI-ACCESS-SIGNATURE': sig,
    'KALSHI-ACCESS-TIMESTAMP': timestamp,
  };
}

async function placeKalshiOrder(ticker, side, sizeUsd, currentPrice) {
  const orderPath = '/trade-api/v2/portfolio/orders';
  const headers   = kalshiAuthHeaders('POST', orderPath);

  if(!headers) {
    log('ERROR', 'Cannot place order — missing KALSHI_API_KEY or KALSHI_PRIVATE_KEY');
    return null;
  }

  // Calculate contracts — each contract costs `price` cents
  // sizeUsd e.g. $5, price e.g. 0.74 = 74 cents = $0.74 per contract
  const pricePerContract = currentPrice || 0.5;
  const contracts = Math.max(1, Math.floor(sizeUsd / pricePerContract));
  const limitPrice = Math.round(pricePerContract * 100) / 100; // round to 2dp

  const orderId = crypto.randomUUID();
  const body = {
    ticker:           ticker,
    client_order_id:  orderId,
    side:             side === 'YES' ? 'yes' : 'no',
    count:            contracts,
    price:            limitPrice.toFixed(2),
    time_in_force:    'fill_or_kill', // fast execution, cancel if not filled immediately
    type:             'limit',
  };

  log('INFO', `Placing Kalshi order: ${ticker} ${side} | ${contracts} contracts @ $${limitPrice} | ~$${sizeUsd}`);

  try {
    const r = await fetch(`${CONFIG.kalshiUrl}${orderPath}`, {
      method:  'POST',
      headers,
      body:    JSON.stringify(body),
    });

    const d = await r.json();

    if(!r.ok) {
      log('ERROR', `Kalshi order failed ${r.status}: ${JSON.stringify(d)}`);
      return null;
    }

    const order = d.order || d;
    log('INFO', `✅ Kalshi order placed: ${order.order_id || orderId} | ${ticker} ${side} | ${contracts} contracts`);
    return order;
  } catch(e) {
    log('ERROR', `placeKalshiOrder: ${e.message}`);
    return null;
  }
}

// ── PROCESS A TRADE ───────────────────────────────────────────
async function processTrade(trade) {
  const { maker, taker, asset_id, price, size, side, id, timestamp } = trade;

  // Determine trader address — could be maker or taker
  const trader = TOP_TRADERS.has(maker) ? maker
               : TOP_TRADERS.has(taker) ? taker
               : null;
  if(!trader) return; // not a top trader

  // Determine coin from asset context (set by market subscription)
  const coin = trade._coin;
  if(!coin) return;

  // Parse side — on Polymarket YES buy = price > 0.5 typically
  const tradeSide = side === 'BUY' ? 'YES' : 'NO';

  // Filter tiny trades
  const sizeUsd = parseFloat(price||0) * parseFloat(size||0) * 100;
  if(sizeUsd < CONFIG.minTradeUsd) return;

  const ts = timestamp ? Math.floor(new Date(timestamp).getTime()/1000) : Math.floor(Date.now()/1000);

  // Store trade
  insertTrade.run({ id: id||`${trader}-${ts}-${Math.random()}`, trader, coin, side:tradeSide, price:parseFloat(price||0), size_usd:sizeUsd, ts });

  log('TRADE', `TOP TRADER ${trader.slice(0,10)}... | ${coin} ${tradeSide} | $${sizeUsd.toFixed(0)} @ ${(parseFloat(price||0)*100).toFixed(1)}¢`);

  // Update signal state
  const key = `${coin}::${tradeSide}`;
  if(!signalState.has(key)) {
    signalState.set(key, { traders: new Set(), firstSeen: ts });
  }
  const state = signalState.get(key);
  state.traders.add(trader);
  state.lastTrade = ts;

  // Check for signal
  const now = Math.floor(Date.now()/1000);
  const age = now - state.firstSeen;

  // Clean old traders (outside time window)
  if(age > CONFIG.windowSecs) {
    state.traders.clear();
    state.firstSeen = ts;
    state.traders.add(trader);
  }

  const traderCount = state.traders.size;
  log('INFO', `Signal state ${key}: ${traderCount}/${CONFIG.minTraders} traders in window`);

  if(traderCount >= CONFIG.minTraders) {
    // Check if in Kalshi alert window
    if(!inKalshiAlertWindow()) {
      log('INFO', `Signal ${key} has ${traderCount} traders but NOT in Kalshi window yet (${Math.round(secsToNextSettlement()/60)}m to settlement)`);
      return;
    }

    // Prevent duplicate alerts for same coin+side in same Kalshi window
    const windowKey = `${key}::${Math.floor(now/900)}`; // 900s = 15min window
    if(alertedThisWindow.has(windowKey)) {
      log('INFO', `Already alerted ${key} this window`);
      return;
    }
    alertedThisWindow.add(windowKey);

    // Fire signal!
    await fireSignal(coin, tradeSide, state.traders, ts);

    // Reset state for this pair after firing
    state.traders.clear();
    state.firstSeen = ts;
  }
}

// ── FIRE SIGNAL ───────────────────────────────────────────────
async function fireSignal(coin, side, traders, ts) {
  const secsLeft  = secsToNextSettlement();
  const settleAt  = nextSettlementTime();
  const traderArr = [...traders];

  log('SIGNAL', `🎯 SIGNAL: ${coin} ${side} | ${traderArr.length} traders | ${secsLeft}s to ${settleAt}`);

  // Look up Kalshi market
  let kalshi = kalshiCache.get(coin);
  if(!kalshi) {
    kalshi = await fetchKalshiMarket(coin);
    if(kalshi) kalshiCache.set(coin, kalshi);
  }

  const kalshiTicker = kalshi?.ticker || `${coin}-15MIN`;
  const kalshiPrice  = kalshi?.price  || null;
  const kalshiTitle  = kalshi?.title  || `${coin} 15-min market`;

  // Store signal
  insertSignal.run({
    coin, side,
    traders:      JSON.stringify(traderArr),
    trader_count: traderArr.length,
    kalshi_ticker: kalshiTicker,
    kalshi_price:  kalshiPrice,
    fired_at:      Math.floor(Date.now()/1000),
    window_end:    Math.floor(Date.now()/1000) + secsLeft,
  });

  // Discord alert
  const priceStr = kalshiPrice ? `${(kalshiPrice*100).toFixed(1)}¢` : 'check app';
  const urgency  = secsLeft < 120 ? '🚨 URGENT' : '🎯 SIGNAL';

  await discord([
    `${urgency} — **${coin} ${side}** on Kalshi`,
    `━━━━━━━━━━━━━━━━━━━━`,
    `📊 **${traderArr.length}** top-100 Polymarket traders bought **${coin} ${side}**`,
    `⏱  **${secsLeft}s left** before Kalshi settles at **${settleAt}**`,
    `━━━━━━━━━━━━━━━━━━━━`,
    `🎯 **Kalshi market:** ${kalshiTitle}`,
    `📋 **Ticker:** \`${kalshiTicker}\``,
    `💰 **Current YES price:** ${priceStr}`,
    `━━━━━━━━━━━━━━━━━━━━`,
    `👉 **Buy ${side} on Kalshi NOW** — ${secsLeft}s window`,
    `🔗 https://kalshi.com/markets/${kalshiTicker.toLowerCase()}`,
  ].join('\n'));

  log('SIGNAL', `Alert fired: ${coin} ${side} → ${kalshiTicker} | ${secsLeft}s left`);

  // ── PLACE REAL ORDER ON KALSHI ──────────────────────────────
  const ORDER_SIZE_USD = parseFloat(process.env.ORDER_SIZE_USD) || 5;
  const order = await placeKalshiOrder(kalshiTicker, side, ORDER_SIZE_USD, kalshiPrice);

  if(order) {
    await discord([
      `✅ **ORDER PLACED — ${coin} ${side}**`,
      `━━━━━━━━━━━━━━━━━━━━`,
      `🎯 **${kalshiTicker}** | **${side}**`,
      `💸 Size: **~$${ORDER_SIZE_USD}** | Price: **${kalshiPrice ? (kalshiPrice*100).toFixed(1)+'¢' : 'market'}**`,
      `🆔 Order ID: \`${order.order_id || 'submitted'}\``,
      `⏱  **${secsLeft}s** until settlement`,
    ].join('\n'));
  } else {
    await discord([
      `❌ **ORDER FAILED — ${coin} ${side}**`,
      `Could not place order on Kalshi — check logs`,
      `Manual entry: https://kalshi.com/markets/${kalshiTicker.toLowerCase()}`,
    ].join('\n'));
  }
}

// ── FETCH ACTIVE POLYMARKET 5-MIN CRYPTO MARKETS ─────────────
async function fetchPolyMarkets() {
  const markets = [];
  // Polymarket 5-min market titles look like:
  // "Bitcoin Up or Down - June 5, 3:50PM-3:55PM ET"
  // "Ethereum Up or Down - June 5, 3:50PM-3:55PM ET"
  // We fetch events from the Gamma API with crypto tag

  const COIN_NAMES = {
    BTC:  ['bitcoin','btc'],
    ETH:  ['ethereum','eth'],
    SOL:  ['solana','sol'],
    XRP:  ['xrp','ripple'],
    DOGE: ['doge','dogecoin'],
    BNB:  ['bnb','binance'],
    HYPE: ['hype','hyperliquid'],
  };

  try {
    // Fetch active crypto events — 5min markets live here
    // Try multiple endpoints
    const urls = [
      `${CONFIG.POLY_REST}/events?active=true&closed=false&tag_id=21&limit=100`,
      `${CONFIG.POLY_REST}/events?active=true&closed=false&limit=200&tag=crypto`,
      `${CONFIG.POLY_REST}/markets?active=true&closed=false&limit=200`,
    ];

    let allMarkets = [];
    for(const url of urls) {
      try {
        const r = await fetch(url, { headers:{ 'Accept':'application/json','User-Agent':'Mozilla/5.0' } });
        if(!r.ok){ log('WARN',`${url} → ${r.status}`); continue; }
        const d = await r.json();
        const items = Array.isArray(d) ? d : (d.data||d.results||d.events||d.markets||[]);
        if(items.length > 0){ allMarkets = items; log('INFO',`Got ${items.length} items from ${url.split('?')[0]}`); break; }
      } catch(e){ log('WARN',`Endpoint ${url}: ${e.message}`); }
    }

    if(!allMarkets.length){
      log('WARN','No markets from REST — will rely on WebSocket trade matching by trader address');
      return [];
    }

    for(const item of allMarkets) {
      // Item could be an event (with nested markets) or a market directly
      const subMarkets = item.markets || [item];

      for(const m of subMarkets) {
        const title  = (m.question||m.title||item.title||'').toLowerCase();
        const is5min = (title.includes('5') && (title.includes('min')||title.includes(':5')||title.includes('3:5')||title.includes('4:5')||title.includes('2:5')||title.includes('1:5')||title.includes('0:5'))) ||
                       title.includes('up or down');
        if(!is5min) continue;
        if(m.active === false || m.closed === true) continue;

        // Match coin
        let coin = null;
        for(const [c, names] of Object.entries(COIN_NAMES)){
          if(names.some(n => title.includes(n))){ coin = c; break; }
        }
        if(!coin) continue;

        // Extract token IDs — Polymarket uses clobTokenIds array
        const clobIds = m.clobTokenIds ? JSON.parse(m.clobTokenIds) : null;
        const yesTokenId = clobIds?.[0] || m.tokens?.[0]?.token_id || m.clob_token_ids?.[0];
        const noTokenId  = clobIds?.[1] || m.tokens?.[1]?.token_id || m.clob_token_ids?.[1];
        const condId     = m.conditionId || m.condition_id;

        markets.push({
          coin,
          title:       m.question || m.title || item.title,
          conditionId: condId,
          yesTokenId,
          noTokenId,
          marketId:    m.id,
        });
        log('INFO', `✅ ${coin} 5-min: ${(m.question||m.title||'').slice(0,60)} | YES:${yesTokenId?.slice(0,8)||'?'}`);
      }
    }
  } catch(e) { log('ERROR', `fetchPolyMarkets: ${e.message}`); }

  log('INFO', `Found ${markets.length} active 5-min crypto markets`);
  return markets;
}

// ── ALSO POLL RECENT TRADES (FALLBACK) ───────────────────────
// Since WebSocket may miss some trades, also poll recent trades for top traders
async function pollRecentTrades(markets) {
  if(!markets.length) return;
  const cutoff = Math.floor(Date.now()/1000) - CONFIG.windowSecs;

  // Sample a subset of top traders to poll (CLOB rate limits)
  const traders = [...TOP_TRADERS].slice(0, 20);

  for(const trader of traders) {
    try {
      const r = await fetch(`${CONFIG.CLOB_REST}/trades?user=${trader}&limit=10`);
      if(!r.ok) continue;
      const d = await r.json();
      const trades = d?.data || d || [];

      for(const t of trades) {
        const ts = t.timestamp ? Math.floor(new Date(t.timestamp).getTime()/1000) : 0;
        if(ts < cutoff) continue;

        // Match to a known market
        const market = markets.find(m =>
          m.conditionId === (t.market||t.condition_id) ||
          m.yesTokenId  === t.asset_id ||
          m.noTokenId   === t.asset_id
        );
        if(!market) continue;

        const side = (market.yesTokenId === t.asset_id ||
                     (t.side||'').toUpperCase()==='BUY') ? 'YES' : 'NO';
        const sizeUsd = parseFloat(t.price||0)*parseFloat(t.size||0)*100;
        if(sizeUsd < CONFIG.minTradeUsd) continue;

        await processTrade({
          ...t,
          maker:  trader,
          taker:  trader,
          _coin:  market.coin,
          side:   side==='YES'?'BUY':'SELL',
          id:     t.id||`${trader}-${ts}`,
          timestamp: t.timestamp,
        });
      }
      await new Promise(r=>setTimeout(r,150)); // rate limit
    } catch(e) {}
  }
}

// ── WEBSOCKET ─────────────────────────────────────────────────
let ws = null;
let wsMarkets = [];

function connectWebSocket(markets) {
  wsMarkets = markets;
  log('INFO', `Connecting to Polymarket WebSocket...`);

  ws = new WebSocket(CONFIG.POLY_WS);

  ws.on('open', () => {
    log('INFO', '✅ Polymarket WebSocket connected');

    // Subscribe to all market token IDs
    const assetIds = markets
      .flatMap(m => [m.yesTokenId, m.noTokenId].filter(Boolean))
      .slice(0, 200); // WS limit

    if(assetIds.length === 0) {
      log('WARN', 'No asset IDs yet — WebSocket open, will subscribe when markets load');
      // Still keep WS open — we'll reconnect with IDs after market refresh
      return;
    }

    ws.send(JSON.stringify({
      auth:    {},
      type:    'Market',
      markets: [],
      assets:  assetIds,
    }));

    log('INFO', `Subscribed to ${assetIds.length} market token IDs`);
  });

  ws.on('message', async (raw) => {
    try {
      const events = JSON.parse(raw.toString());
      const list   = Array.isArray(events) ? events : [events];

      for(const event of list) {
        if(event.event_type !== 'trade') continue;

        // Find which market this trade belongs to
        const market = wsMarkets.find(m =>
          m.yesTokenId === event.asset_id ||
          m.noTokenId  === event.asset_id
        );
        if(!market) continue;

        // Check if either side is a top trader
        const isTopMaker = TOP_TRADERS.has(event.maker_address);
        const isTopTaker = TOP_TRADERS.has(event.taker_address);
        if(!isTopMaker && !isTopTaker) continue;

        const side = (market.yesTokenId === event.asset_id) ? 'BUY' : 'SELL';

        await processTrade({
          id:        event.id,
          maker:     event.maker_address,
          taker:     event.taker_address,
          asset_id:  event.asset_id,
          price:     event.price,
          size:      event.size,
          side,
          timestamp: event.timestamp,
          _coin:     market.coin,
        });
      }
    } catch(e) { log('ERROR', `WS message: ${e.message}`); }
  });

  ws.on('error', e => log('ERROR', `WS error: ${e.message}`));

  ws.on('close', (code) => {
    log('INFO', `WS closed (${code}) — reconnecting in 5s`);
    setTimeout(() => connectWebSocket(wsMarkets), 5000);
  });

  // Keepalive ping every 30s
  setInterval(() => {
    if(ws?.readyState === WebSocket.OPEN) ws.ping();
  }, 30000);
}

// ── MAIN ──────────────────────────────────────────────────────
async function main() {
  console.log('\n╔══════════════════════════════════════════════════════════════╗');
  console.log('║  🎯 POLYMARKET → KALSHI SIGNAL BOT v2                        ║');
  console.log('║  Real-time WebSocket | Top 100 traders | 15-min crypto       ║');
  console.log('╚══════════════════════════════════════════════════════════════╝\n');

  // Add any extra traders from env var
  if(process.env.TRADER_ADDRESSES) {
    const extra = process.env.TRADER_ADDRESSES.split(',').map(a=>a.trim().toLowerCase()).filter(Boolean);
    extra.forEach(a => TOP_TRADERS.add(a));
    log('INFO', `Loaded ${extra.length} extra traders from TRADER_ADDRESSES`);
  }

  log('INFO', `Watching ${TOP_TRADERS.size} top traders`);
  log('INFO', `Signal: ${CONFIG.minTraders}+ traders same side within ${CONFIG.windowSecs/60}min`);
  log('INFO', `Alert window: last ${CONFIG.kalshiWindowMin}min before Kalshi settlement`);
  log('INFO', `Min trade size: $${CONFIG.minTradeUsd}`);

  await discord(
    `🎯 **Polymarket → Kalshi Bot ONLINE**\n` +
    `━━━━━━━━━━━━━━━━━━━━\n` +
    `👥 Watching **${TOP_TRADERS.size}** top Polymarket crypto traders\n` +
    `📡 Real-time WebSocket feed\n` +
    `⚡ Signal: **${CONFIG.minTraders}+** traders same side within **${CONFIG.windowSecs/60}min**\n` +
    `⏱  Alerts fire in **last ${CONFIG.kalshiWindowMin}min** before Kalshi settlement\n` +
    `🪙 Coins: **${CONFIG.COINS.join(', ')}**`
  );

  // Fetch active markets
  log('INFO', 'Fetching active Polymarket 5-min crypto markets...');
  const markets = await fetchPolyMarkets();

  if(markets.length === 0) {
    log('WARN', 'No 5-min markets found via REST — WebSocket will still listen for any trades from top traders');
    log('WARN', 'This can happen if markets are between windows — will retry in 5min');
  }

  // Connect WebSocket
  connectWebSocket(markets);

  // Refresh markets every 5 minutes — update list WITHOUT closing WebSocket
  setInterval(async () => {
    log('INFO', 'Refreshing market list...');
    const fresh = await fetchPolyMarkets();
    if(fresh.length > 0) {
      wsMarkets = fresh;
      // Re-subscribe with new token IDs without closing the connection
      if(ws?.readyState === WebSocket.OPEN) {
        const newIds = fresh
          .flatMap(m => [m.yesTokenId, m.noTokenId].filter(Boolean))
          .slice(0, 200);
        if(newIds.length > 0) {
          ws.send(JSON.stringify({ auth:{}, type:'Market', markets:[], assets:newIds }));
          log('INFO', `Re-subscribed to ${newIds.length} token IDs (no reconnect needed)`);
        }
      }
    }
    kalshiCache.clear();
    alertedThisWindow.clear();
  }, 5 * 60 * 1000);

  // Also poll every 60s as fallback (catches trades WS missed)
  setInterval(async () => {
    if(wsMarkets.length > 0) {
      await pollRecentTrades(wsMarkets);
    }
  }, 60 * 1000);

  // Status log every 5min
  setInterval(() => {
    const secsLeft = secsToNextSettlement();
    const inWindow = inKalshiAlertWindow();
    log('INFO', `Status | Next settlement: ${nextSettlementTime()} (${secsLeft}s) | In alert window: ${inWindow} | Markets watched: ${wsMarkets.length}`);

    // Log current signal states
    for(const [key, state] of signalState) {
      if(state.traders.size > 0) {
        log('INFO', `  Signal state: ${key} = ${state.traders.size} traders`);
      }
    }
  }, 5 * 60 * 1000);

  log('INFO', `✅ Bot running — LIVE ORDER MODE | $${parseFloat(process.env.ORDER_SIZE_USD)||5} per trade`);
}

process.on('SIGINT', async () => {
  log('INFO', 'Shutting down...');
  await discord('🔴 **Polymarket → Kalshi Bot OFFLINE**');
  process.exit(0);
});

process.on('unhandledRejection', e => log('ERROR', `Unhandled: ${e.message}`));

main().catch(e => {
  log('ERROR', `Fatal: ${e.message}`);
  process.exit(1);
});
