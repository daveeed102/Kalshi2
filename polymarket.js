const fetch  = require('node-fetch');
const log    = require('./logger');
const db     = require('./db');
const config = require('./config');

const GAMMA_BASE = 'https://gamma-api.polymarket.com';
const CLOB_BASE  = 'https://clob.polymarket.com';

const sleep = ms => new Promise(r => setTimeout(r, ms));

// ── LEADERBOARD ───────────────────────────────────────────────
// Polymarket Gamma API has leaderboard data

async function fetchLeaderboard() {
  try {
    log.poll('Fetching Polymarket leaderboard...');

    // Gamma API leaderboard — sorted by profit
    const r = await fetch(
      `${GAMMA_BASE}/leaderboard?window=monthly&limit=${config.signal.leaderboardSize}`,
      { headers: { 'Accept': 'application/json' } }
    );

    if(!r.ok) {
      // Fallback: try the profiles endpoint
      log.warn(`Gamma leaderboard ${r.status} — trying CLOB fallback`);
      return await fetchLeaderboardCLOB();
    }

    const data = await r.json();
    const traders = (data.data || data || []).map((t, i) => ({
      id:         t.proxyWalletAddress || t.address || t.user,
      rank:       t.rank || i + 1,
      profit_30d: parseFloat(t.pnl || t.profit || 0),
      volume_30d: parseFloat(t.volume || 0),
      fetched_at: Math.floor(Date.now()/1000),
    })).filter(t => t.id);

    db.bulkUpsertTraders(traders);
    log.trader(`Leaderboard updated — ${traders.length} traders stored`);
    return traders;
  } catch(e) {
    log.error(`fetchLeaderboard: ${e.message}`);
    return [];
  }
}

async function fetchLeaderboardCLOB() {
  try {
    const r = await fetch(
      `${CLOB_BASE}/profiles/leaderboard?window=monthly&limit=${config.signal.leaderboardSize}`
    );
    if(!r.ok) throw new Error(`CLOB leaderboard ${r.status}`);
    const data = await r.json();
    return (data.data || data || []).map((t, i) => ({
      id:         t.proxyWalletAddress || t.address,
      rank:       i + 1,
      profit_30d: parseFloat(t.pnl || 0),
      volume_30d: parseFloat(t.volume || 0),
      fetched_at: Math.floor(Date.now()/1000),
    })).filter(t => t.id);
  } catch(e) {
    log.error(`fetchLeaderboardCLOB: ${e.message}`);
    return [];
  }
}

// ── TRADER TRADES ─────────────────────────────────────────────
// Fetch recent trades for a single trader address

async function fetchTraderTrades(address, limit=100) {
  try {
    // CLOB API: trades by user
    const url = `${CLOB_BASE}/trades?user=${address}&limit=${limit}`;
    const r   = await fetch(url, { headers: { 'Accept': 'application/json' } });

    if(!r.ok) {
      log.warn(`Trades for ${address.slice(0,10)} returned ${r.status}`);
      return [];
    }

    const data   = await r.json();
    const trades = (data.data || data || []);

    return trades.map(t => ({
      id:             t.id || `${address}-${t.timestamp}-${Math.random()}`,
      trader_address: address,
      market_id:      t.market || t.marketId || t.condition_id || '',
      market_title:   t.title  || t.market_title || '',
      outcome:        t.outcome_index === 0 ? 'YES' : t.outcome_index === 1 ? 'NO' : (t.outcome||'YES'),
      side:           (t.side||t.type||'BUY').toUpperCase().includes('BUY') ? 'BUY' : 'SELL',
      price:          parseFloat(t.price || 0),
      size:           parseFloat(t.size  || t.matched || 0),
      usd_value:      parseFloat(t.usdcSize || t.size_matched_usd || (parseFloat(t.price||0) * parseFloat(t.size||0))),
      timestamp:      t.timestamp ? Math.floor(new Date(t.timestamp).getTime()/1000) : Math.floor(Date.now()/1000),
    }));
  } catch(e) {
    log.error(`fetchTraderTrades(${address.slice(0,10)}): ${e.message}`);
    return [];
  }
}

// ── POLL ALL TRADERS ──────────────────────────────────────────
// Fetch trades for all top-50 traders with rate limiting

async function pollAllTraderTrades() {
  const traders = db.getTopTraders.all(config.signal.leaderboardSize);
  if(!traders.length) {
    log.warn('No traders in DB — run fetchLeaderboard first');
    return 0;
  }

  log.poll(`Polling trades for ${traders.length} traders...`);
  let totalNew = 0;

  for(const trader of traders) {
    const trades = await fetchTraderTrades(trader.id, 50);
    if(trades.length > 0) {
      db.bulkUpsertTrades(trades);
      totalNew += trades.length;
    }
    await sleep(200); // 200ms rate limit = ~10s for 50 traders
  }

  log.poll(`Poll complete — ${totalNew} trades stored`);
  return totalNew;
}

// ── MARKET DETAIL ─────────────────────────────────────────────

async function fetchMarketDetail(conditionId) {
  try {
    const r = await fetch(`${GAMMA_BASE}/markets?condition_ids=${conditionId}`);
    if(!r.ok) return null;
    const data = await r.json();
    return (data.data || data || [])[0] || null;
  } catch(e) { return null; }
}

module.exports = {
  fetchLeaderboard,
  fetchTraderTrades,
  pollAllTraderTrades,
  fetchMarketDetail,
};
