// ============================================================
// KALSHI COPY TRADER v3
// - Watches ALL trades on Kalshi 15-min crypto markets
// - Tracks win rates per trader
// - When a high-win-rate trader bets → we copy for $5
// - No Polymarket, no leaderboard scraping, no external deps
// ============================================================
require('dotenv').config();
const fs       = require('fs');
const path     = require('path');
const fetch    = require('node-fetch');
const WebSocket = require('ws');
const Database = require('better-sqlite3');
const crypto   = require('crypto');

// ── CONFIG ────────────────────────────────────────────────────
const CONFIG = {
  kalshiBase:   process.env.KALSHI_BASE_URL    || 'https://api.elections.kalshi.com',
  kalshiKey:    process.env.KALSHI_API_KEY      || '',
  kalshiSecret: process.env.KALSHI_PRIVATE_KEY  || '',
  discord:      process.env.DISCORD_WEBHOOK_URL || '',

  ORDER_SIZE_USD:    parseFloat(process.env.ORDER_SIZE_USD)    || 5,
  MIN_WIN_RATE:      parseFloat(process.env.MIN_WIN_RATE)      || 0.60, // 60% win rate to copy
  MIN_TRADES:        parseInt(process.env.MIN_TRADES)          || 5,    // must have at least 5 settled trades
  MAX_COPY_PRICE:    parseFloat(process.env.MAX_COPY_PRICE)    || 0.90, // don't buy if > 90¢
  MIN_COPY_PRICE:    parseFloat(process.env.MIN_COPY_PRICE)    || 0.10, // don't buy if < 10¢

  // Kalshi series tickers for 15-min crypto markets
  SERIES: {
    BTC:  'KXBTC15M',
    ETH:  'KXETH15M',
    SOL:  'KXSOL15M',
    XRP:  'KXXRP15M',
    DOGE: 'KXDOGE15M',
  },
};

// ── LOGGER ────────────────────────────────────────────────────
fs.mkdirSync(path.join(__dirname,'logs'), { recursive:true });
const logFile = path.join(__dirname,'logs','bot.log');
function log(level, msg) {
  const line = `[${new Date().toISOString()}] [${level}] ${msg}`;
  console.log(line);
  try { fs.appendFileSync(logFile, line+'\n'); } catch(e) {}
}

// ── DATABASE ──────────────────────────────────────────────────
fs.mkdirSync(path.join(__dirname,'data'), { recursive:true });
const db = new Database(path.join(__dirname,'data','traders.db'));
db.pragma('journal_mode = WAL');
db.exec(`
  CREATE TABLE IF NOT EXISTS trader_stats (
    member_id   TEXT PRIMARY KEY,
    wins        INTEGER DEFAULT 0,
    losses      INTEGER DEFAULT 0,
    total_pnl   REAL    DEFAULT 0,
    last_seen   INTEGER DEFAULT 0
  );
  CREATE TABLE IF NOT EXISTS market_trades (
    id          TEXT PRIMARY KEY,
    member_id   TEXT,
    ticker      TEXT,
    side        TEXT,
    price       INTEGER,
    count       INTEGER,
    ts          INTEGER
  );
  CREATE TABLE IF NOT EXISTS our_trades (
    id          TEXT PRIMARY KEY,
    ticker      TEXT,
    side        TEXT,
    entry_price INTEGER,
    count       INTEGER,
    size_usd    REAL,
    opened_at   INTEGER,
    closed_at   INTEGER,
    exit_price  INTEGER,
    pnl_usd     REAL,
    status      TEXT DEFAULT 'OPEN',
    copied_from TEXT
  );
`);

const upsertTrader = db.prepare(`
  INSERT INTO trader_stats (member_id, wins, losses, total_pnl, last_seen)
  VALUES (@member_id, 0, 0, 0, @last_seen)
  ON CONFLICT(member_id) DO UPDATE SET last_seen=@last_seen
`);
const addWin  = db.prepare(`UPDATE trader_stats SET wins=wins+1,   total_pnl=total_pnl+@pnl WHERE member_id=@id`);
const addLoss = db.prepare(`UPDATE trader_stats SET losses=losses+1, total_pnl=total_pnl+@pnl WHERE member_id=@id`);
const getTrader = db.prepare(`SELECT * FROM trader_stats WHERE member_id=?`);
const insertMarketTrade = db.prepare(`INSERT OR IGNORE INTO market_trades VALUES (@id,@member_id,@ticker,@side,@price,@count,@ts)`);
const insertOurTrade  = db.prepare(`INSERT INTO our_trades VALUES (@id,@ticker,@side,@entry_price,@count,@size_usd,@opened_at,null,null,null,'OPEN',@copied_from)`);
const updateOurTrade  = db.prepare(`UPDATE our_trades SET status=@status,closed_at=@closed_at,exit_price=@exit_price,pnl_usd=@pnl_usd WHERE id=@id`);
const getOpenTrades   = db.prepare(`SELECT * FROM our_trades WHERE status='OPEN'`);

// ── DISCORD ───────────────────────────────────────────────────
async function discord(msg) {
  if(!CONFIG.discord) return;
  try {
    await fetch(CONFIG.discord, {
      method:'POST', headers:{'Content-Type':'application/json'},
      body: JSON.stringify({ content: msg.slice(0,1990) }),
    });
  } catch(e) {}
}

// ── KALSHI AUTH ───────────────────────────────────────────────
function kalshiHeaders(method, urlPath) {
  const ts  = Date.now().toString();
  const key = CONFIG.kalshiSecret;
  const id  = CONFIG.kalshiKey;
  if(!key || !id) return { 'Content-Type':'application/json' };

  try {
    const msg = ts + method.toUpperCase() + urlPath.split('?')[0];
    const sig = crypto.createSign('SHA256');
    sig.update(msg);
    const signature = sig.sign({
      key: key,
      padding: crypto.constants.RSA_PKCS1_PSS_PADDING,
      saltLength: crypto.constants.RSA_PSS_SALTLEN_DIGEST,
    }, 'base64');
    return {
      'Content-Type':           'application/json',
      'KALSHI-ACCESS-KEY':      id,
      'KALSHI-ACCESS-TIMESTAMP': ts,
      'KALSHI-ACCESS-SIGNATURE': signature,
    };
  } catch(e) {
    log('ERROR', `Auth sign failed: ${e.message}`);
    return { 'Content-Type':'application/json' };
  }
}

// ── GET ACTIVE KALSHI MARKET ──────────────────────────────────
const marketCache = new Map(); // series → { ticker, yesAsk, yesBid }

async function getActiveTicker(series) {
  try {
    const r = await fetch(
      `${CONFIG.kalshiBase}/trade-api/v2/markets?series_ticker=${series}&status=open&limit=5`
    );
    if(!r.ok) return null;
    const d  = await r.json();
    const ms = d.markets || [];
    if(!ms.length) return null;

    const now = Math.floor(Date.now()/1000);
    // Find soonest closing market
    const sorted = ms
      .map(m => {
        const ct = m.close_time;
        const t  = ct ? (typeof ct==='string'
          ? Math.floor(new Date(ct).getTime()/1000)
          : parseInt(ct) > 1e10 ? Math.floor(parseInt(ct)/1000) : parseInt(ct)) : 0;
        return { ticker: m.ticker, closeTime: t, yesAsk: m.yes_ask, yesBid: m.yes_bid, lastPrice: m.last_price };
      })
      .filter(m => m.closeTime > now - 60)
      .sort((a,b) => a.closeTime - b.closeTime);

    return sorted[0] || { ticker: ms[0].ticker, yesAsk: ms[0].yes_ask, yesBid: ms[0].yes_bid };
  } catch(e) {
    log('ERROR', `getActiveTicker(${series}): ${e.message}`);
    return null;
  }
}

async function refreshMarkets() {
  for(const [coin, series] of Object.entries(CONFIG.SERIES)) {
    const m = await getActiveTicker(series);
    if(m) {
      marketCache.set(series, m);
      log('INFO', `✅ ${coin}: ${m.ticker} | YES ask:${m.yesAsk||'?'}¢ bid:${m.yesBid||'?'}¢`);
    } else {
      log('WARN', `No active market for ${coin} (${series})`);
    }
  }
  log('INFO', `Markets loaded: ${marketCache.size}/${Object.keys(CONFIG.SERIES).length}`);
}

// ── PLACE ORDER ───────────────────────────────────────────────
async function placeOrder(ticker, side, priceCents, sizeUsd, copiedFrom) {
  const orderPath = '/trade-api/v2/portfolio/orders';
  const contracts = Math.max(1, Math.floor(sizeUsd / (priceCents / 100)));
  const orderId   = crypto.randomUUID();

  const body = {
    ticker,
    client_order_id: orderId,
    action: 'buy',
    side:   side.toLowerCase(),
    count:  contracts,
    type:   'limit',
    time_in_force: 'fill_or_kill',
  };
  if(side.toLowerCase() === 'yes') body.yes_price = priceCents;
  else                              body.no_price  = priceCents;

  log('INFO', `Placing order: ${ticker} ${side} ${contracts}x @ ${priceCents}¢ (~$${sizeUsd})`);

  try {
    const r = await fetch(`${CONFIG.kalshiBase}${orderPath}`, {
      method: 'POST',
      headers: kalshiHeaders('POST', orderPath),
      body: JSON.stringify(body),
    });
    const d = await r.json();
    if(!r.ok) {
      log('ERROR', `Order failed ${r.status}: ${JSON.stringify(d)}`);
      return null;
    }

    const order = d.order || d;
    log('INFO', `✅ Order placed: ${order.order_id || orderId}`);

    // Record in DB
    insertOurTrade.run({
      id:          orderId,
      ticker,
      side,
      entry_price: priceCents,
      count:       contracts,
      size_usd:    sizeUsd,
      opened_at:   Math.floor(Date.now()/1000),
      copied_from: copiedFrom,
    });

    return order;
  } catch(e) {
    log('ERROR', `placeOrder: ${e.message}`);
    return null;
  }
}

// ── TRADE PROCESSOR ───────────────────────────────────────────
// Called when we see a trade on a Kalshi 15-min market
// Decides whether to copy it based on trader win rate

const recentlyCopied = new Map(); // ticker+side → timestamp, prevent duplicate copies

async function onKalshiTrade(trade) {
  const { market_ticker, member_id, yes_price, no_price, count, action, created_time } = trade;
  if(!member_id || action !== 'buy') return;

  // Figure out which series this belongs to
  let coin = null;
  for(const [c, series] of Object.entries(CONFIG.SERIES)) {
    if(market_ticker.startsWith(series.replace('15M',''))) { coin = c; break; }
    if(market_ticker.includes(c)) { coin = c; break; }
  }
  if(!coin) return;

  const series  = CONFIG.SERIES[coin];
  const priceCents = yes_price || no_price;
  const side    = yes_price ? 'yes' : 'no';
  const ts      = Math.floor(Date.now()/1000);

  // Record this trader
  upsertTrader.run({ member_id, last_seen: ts });
  insertMarketTrade.run({
    id:        `${member_id}-${ts}-${Math.random()}`,
    member_id,
    ticker:    market_ticker,
    side,
    price:     priceCents,
    count:     count || 1,
    ts,
  });

  // Get trader stats
  const stats  = getTrader.get(member_id);
  const total  = (stats?.wins||0) + (stats?.losses||0);
  const winRate = total >= CONFIG.MIN_TRADES ? (stats.wins / total) : null;

  log('INFO',
    `Trade: ${coin} ${side} @ ${priceCents}¢ | trader:${member_id.slice(0,12)} | ` +
    `${total} trades, WR:${winRate !== null ? (winRate*100).toFixed(0)+'%' : 'new'}`
  );

  // Skip price extremes
  if(priceCents > CONFIG.MAX_COPY_PRICE * 100 || priceCents < CONFIG.MIN_COPY_PRICE * 100) {
    log('INFO', `  Skipping — price ${priceCents}¢ outside range`);
    return;
  }

  // Copy if win rate is good enough OR if they're new (give benefit of doubt)
  const shouldCopy = winRate === null || winRate >= CONFIG.MIN_WIN_RATE;
  if(!shouldCopy) {
    log('INFO', `  Skipping — WR ${(winRate*100).toFixed(0)}% below ${CONFIG.MIN_WIN_RATE*100}%`);
    return;
  }

  // Prevent copying same ticker+side twice within 60s
  const copyKey = `${market_ticker}::${side}`;
  const lastCopy = recentlyCopied.get(copyKey) || 0;
  if(ts - lastCopy < 60) {
    log('INFO', `  Skipping — already copied ${copyKey} ${ts-lastCopy}s ago`);
    return;
  }
  recentlyCopied.set(copyKey, ts);

  // Place our order
  const label = winRate !== null
    ? `${(winRate*100).toFixed(0)}% WR over ${total} trades`
    : `new trader`;

  log('INFO', `  🎯 COPYING: ${coin} ${side} @ ${priceCents}¢ (${label})`);

  await discord([
    `🎯 **COPYING TRADE — ${coin.toUpperCase()} ${side.toUpperCase()}**`,
    `━━━━━━━━━━━━━━━━━━━━`,
    `📊 Market: **${market_ticker}**`,
    `💰 Price: **${priceCents}¢**`,
    `👤 Trader: \`${member_id.slice(0,16)}\` | **${label}**`,
    `💸 Our bet: **$${CONFIG.ORDER_SIZE_USD}**`,
  ].join('\n'));

  const order = await placeOrder(market_ticker, side, priceCents, CONFIG.ORDER_SIZE_USD, member_id);

  if(order) {
    await discord([
      `✅ **ORDER PLACED — ${coin.toUpperCase()} ${side.toUpperCase()}**`,
      `🎯 **${market_ticker}** | ${priceCents}¢ | $${CONFIG.ORDER_SIZE_USD}`,
      `🆔 \`${order.order_id || 'submitted'}\``,
    ].join('\n'));
  } else {
    await discord(`❌ **ORDER FAILED** — ${market_ticker} ${side} — check logs`);
  }
}

// ── WEBSOCKET — KALSHI MARKET DATA ───────────────────────────
// Subscribe to all 15-min crypto tickers
// Kalshi WS is public for market data — no auth needed

let ws = null;

function connectKalshiWS() {
  const wsUrl = `${CONFIG.kalshiBase.replace('https','wss').replace('http','ws')}/trade-api/ws/v2`;
  log('INFO', `Connecting to Kalshi WebSocket: ${wsUrl}`);

  // Kalshi WS requires auth headers
  const wsTs  = Date.now().toString();
  const wsPath = '/trade-api/ws/v2';
  let wsHeaders = {};
  if(CONFIG.kalshiKey && CONFIG.kalshiSecret) {
    try {
      const msg = wsTs + 'GET' + wsPath;
      const sig = crypto.createSign('SHA256');
      sig.update(msg);
      const signature = sig.sign({
        key: CONFIG.kalshiSecret,
        padding: crypto.constants.RSA_PKCS1_PSS_PADDING,
        saltLength: crypto.constants.RSA_PSS_SALTLEN_DIGEST,
      }, 'base64');
      wsHeaders = {
        'KALSHI-ACCESS-KEY':       CONFIG.kalshiKey,
        'KALSHI-ACCESS-TIMESTAMP': wsTs,
        'KALSHI-ACCESS-SIGNATURE': signature,
      };
    } catch(e) { log('ERROR', `WS auth: ${e.message}`); }
  }
  ws = new WebSocket(wsUrl, { headers: wsHeaders });

  ws.on('open', () => {
    log('INFO', '✅ Kalshi WebSocket connected');

    // Subscribe to fills (trades) on all series
    const tickers = Object.values(marketCache)
      .map(m => m.ticker)
      .filter(Boolean);

    if(!tickers.length) {
      log('WARN', 'No market tickers yet — will resubscribe after market refresh');
      return;
    }

    // Subscribe to trade feed for each market
    for(const ticker of tickers) {
      ws.send(JSON.stringify({
        id:   ticker,
        cmd:  'subscribe',
        params: {
          channels: ['trade'],
          market_tickers: [ticker],
        },
      }));
    }
    log('INFO', `Subscribed to trade feed for ${tickers.length} markets: ${tickers.join(', ')}`);
  });

  ws.on('message', async (raw) => {
    try {
      const msg = JSON.parse(raw.toString());

      // Handle trade events
      if(msg.type === 'trade' || msg.channel === 'trade') {
        const data = msg.msg || msg.data || msg;
        if(data.market_ticker) {
          await onKalshiTrade(data);
        }
      }

      // Handle orderbook/ticker snapshots — extract recent trades
      if(msg.type === 'fills' || msg.channel === 'fills') {
        const fills = msg.msg?.fills || msg.data?.fills || [];
        for(const fill of fills) {
          await onKalshiTrade({ ...fill, action: 'buy' });
        }
      }
    } catch(e) {
      if(!e.message.includes('JSON')) {
        log('ERROR', `WS message: ${e.message}`);
      }
    }
  });

  ws.on('error', e => log('ERROR', `WS error: ${e.message}`));

  ws.on('close', (code) => {
    log('INFO', `WS closed (${code}) — reconnecting in 5s`);
    setTimeout(connectKalshiWS, 5000);
  });

  // Keepalive
  setInterval(() => {
    if(ws?.readyState === WebSocket.OPEN) ws.ping();
  }, 20000);
}

// ── POLL RECENT KALSHI TRADES (FALLBACK) ─────────────────────
// In case WebSocket misses trades, poll the public trades endpoint
// GET /trade-api/v2/markets/{ticker}/trades — NO auth needed

async function pollKalshiTrades() {
  for(const [coin, series] of Object.entries(CONFIG.SERIES)) {
    const market = marketCache.get(series);
    if(!market?.ticker) continue;

    try {
      const r = await fetch(
        `${CONFIG.kalshiBase}/trade-api/v2/markets/${market.ticker}/trades?limit=10`
      );
      if(!r.ok) continue;
      const d      = await r.json();
      const trades = d.trades || [];
      const cutoff = Math.floor(Date.now()/1000) - 90; // last 90 seconds

      for(const t of trades) {
        const ts = t.created_time
          ? Math.floor(new Date(t.created_time).getTime()/1000)
          : 0;
        if(ts < cutoff) continue;
        await onKalshiTrade({ ...t, market_ticker: market.ticker });
      }
    } catch(e) {
      log('WARN', `pollKalshiTrades(${coin}): ${e.message}`);
    }
    await new Promise(r => setTimeout(r, 100));
  }
}

// ── SETTLE OPEN TRADES ────────────────────────────────────────
// Check if any of our open trades have resolved and calculate PnL

async function settleOpenTrades() {
  const open = getOpenTrades.all();
  if(!open.length) return;

  for(const trade of open) {
    try {
      const r = await fetch(
        `${CONFIG.kalshiBase}/trade-api/v2/markets/${trade.ticker}`
      );
      if(!r.ok) continue;
      const d = await r.json();
      const m = d.market || d;

      // Market resolved?
      if(m.status === 'finalized' || m.status === 'settled') {
        const wonYes = m.result === 'yes';
        const ourSide = trade.side.toLowerCase();
        const won = (ourSide === 'yes' && wonYes) || (ourSide === 'no' && !wonYes);

        const exitPrice = won ? 100 : 0;
        const pnl = won
          ? (trade.count * ((100 - trade.entry_price) / 100))
          : -(trade.count * (trade.entry_price / 100));

        updateOurTrade.run({
          id:         trade.id,
          status:     'CLOSED',
          closed_at:  Math.floor(Date.now()/1000),
          exit_price: exitPrice,
          pnl_usd:    pnl,
        });

        // Update trader win/loss record
        if(trade.copied_from) {
          if(won) addWin.run({ id: trade.copied_from, pnl });
          else    addLoss.run({ id: trade.copied_from, pnl });
        }

        const sign = pnl >= 0 ? '+' : '';
        log('INFO', `Trade settled: ${trade.ticker} ${trade.side} | ${won?'WIN':'LOSS'} | ${sign}$${pnl.toFixed(2)}`);

        await discord([
          `${won ? '✅ WIN' : '❌ LOSS'} — **${trade.ticker} ${trade.side.toUpperCase()}**`,
          `━━━━━━━━━━━━━━━━━━━━`,
          `📥 Entry: **${trade.entry_price}¢** | Exit: **${exitPrice}¢**`,
          `${pnl>=0?'📈':'📉'} PnL: **${sign}$${pnl.toFixed(2)}**`,
          `👤 Copied from: \`${(trade.copied_from||'').slice(0,16)}\``,
        ].join('\n'));
      }
    } catch(e) {}
  }
}

// ── MAIN ──────────────────────────────────────────────────────
async function main() {
  console.log('\n╔══════════════════════════════════════════════════════════════╗');
  console.log('║  🎯 KALSHI COPY TRADER v3                                     ║');
  console.log('║  Watches Kalshi trades → copies winning traders for $5        ║');
  console.log('╚══════════════════════════════════════════════════════════════╝\n');

  log('INFO', `Copy threshold: ${CONFIG.MIN_WIN_RATE*100}% WR after ${CONFIG.MIN_TRADES} trades`);
  log('INFO', `Order size: $${CONFIG.ORDER_SIZE_USD} | Price range: ${CONFIG.MIN_COPY_PRICE*100}-${CONFIG.MAX_COPY_PRICE*100}¢`);
  log('INFO', `New traders: copying by default until we have data on them`);

  if(!CONFIG.kalshiKey) log('WARN', 'No KALSHI_API_KEY — will watch only, cannot place orders');

  // Load markets
  await refreshMarkets();

  // Connect WebSocket
  connectKalshiWS();

  await discord([
    `🎯 **KALSHI COPY TRADER v3 ONLINE**`,
    `━━━━━━━━━━━━━━━━━━━━`,
    `📊 Watching: **${Object.keys(CONFIG.SERIES).join(', ')}** 15-min markets`,
    `🎯 Copying traders with **≥${CONFIG.MIN_WIN_RATE*100}%** WR`,
    `💸 **$${CONFIG.ORDER_SIZE_USD}** per copy trade`,
    `🆕 New traders: **copied by default**`,
    `⚡ Real-time Kalshi WebSocket + polling fallback`,
  ].join('\n'));

  // Refresh markets every 5 minutes (new windows open)
  setInterval(async () => {
    await refreshMarkets();
    // Reconnect WS with updated tickers
    if(ws?.readyState === WebSocket.OPEN) {
      ws.close(1000, 'refresh');
    }
  }, 5 * 60 * 1000);

  // Poll trades every 10s — primary detection mechanism
  setInterval(pollKalshiTrades, 10 * 1000);
  // Also run immediately
  setTimeout(pollKalshiTrades, 2000);

  // Settle open trades every 2 minutes
  setInterval(settleOpenTrades, 2 * 60 * 1000);

  // Status every 5 minutes
  setInterval(() => {
    const open = getOpenTrades.all();
    log('INFO', `Status | Open trades: ${open.length} | Markets: ${marketCache.size}/5`);
  }, 5 * 60 * 1000);

  log('INFO', '✅ Running — watching Kalshi trades...');
}

process.on('SIGINT', async () => {
  log('INFO', 'Shutting down...');
  await discord('🔴 **Kalshi Copy Trader OFFLINE**');
  process.exit(0);
});

process.on('unhandledRejection', e => log('ERROR', `Unhandled: ${e.message}`));
main().catch(e => { log('ERROR', `Fatal: ${e.message}`); process.exit(1); });
