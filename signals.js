const { v4: uuidv4 } = require('uuid');
const Fuse           = require('fuse.js');
const log            = require('./logger');
const db             = require('./db');
const config         = require('./config');

// ── SIGNAL DETECTION ──────────────────────────────────────────
// Called after every poll cycle
// Groups recent buys by market and checks for 2+ trader consensus

async function detectSignals() {
  const windowSecs = config.signal.timeWindowMinutes * 60;
  const cutoff     = Math.floor(Date.now()/1000) - windowSecs;
  const minTraders = config.signal.minTraders;

  // Get all recent buys within time window
  const recentBuys = db.getRecentBuys.all(cutoff);
  if(!recentBuys.length) return [];

  // Get set of known top-50 trader addresses
  const topTraders = new Set(
    db.getTopTraders.all(config.signal.leaderboardSize).map(t => t.id)
  );

  // Filter to only top-50 trader buys
  const topBuys = recentBuys.filter(t => topTraders.has(t.trader_address));
  if(!topBuys.length) return [];

  // ── CLUSTER by market_id (exact) ──────────────────────────
  const byMarket = new Map();
  for(const trade of topBuys) {
    const key = `${trade.market_id}::${trade.outcome}`;
    if(!byMarket.has(key)) byMarket.set(key, []);
    byMarket.get(key).push(trade);
  }

  // ── ALSO CLUSTER by title similarity (fuzzy) ──────────────
  // For trades with no market_id or different IDs but same event
  const titleGroups = clusterByTitle(topBuys);

  // Merge both clustering approaches
  const allGroups = new Map([...byMarket, ...titleGroups]);

  const newSignals = [];

  for(const [key, trades] of allGroups) {
    // Deduplicate by trader (one trader may buy same market multiple times)
    const byTrader = new Map();
    for(const t of trades) {
      if(!byTrader.has(t.trader_address)) byTrader.set(t.trader_address, t);
    }

    const uniqueTraders = [...byTrader.values()];
    if(uniqueTraders.length < minTraders) continue;

    // Extract market info from best trade
    const best = uniqueTraders.reduce((a, b) => (b.usd_value > a.usd_value ? b : a));

    // Check if we already signaled this market recently (24h dedup)
    const existing = db.signalExistsForMarket.get(
      best.market_id,
      best.outcome,
      Math.floor(Date.now()/1000) - 86400
    );
    if(existing) {
      log.skip(`Already signaled ${best.market_title?.slice(0,40)} in last 24h`);
      continue;
    }

    const avgPrice = uniqueTraders.reduce((s, t) => s + t.price, 0) / uniqueTraders.length;
    const signalId = uuidv4();

    const signal = {
      id:             signalId,
      market_id:      best.market_id,
      market_title:   best.market_title || best.market_id,
      outcome:        best.outcome,
      traders:        JSON.stringify(uniqueTraders.map(t => t.trader_address)),
      trader_count:   uniqueTraders.length,
      avg_price:      avgPrice,
      window_minutes: config.signal.timeWindowMinutes,
      detected_at:    Math.floor(Date.now()/1000),
      status:         'PENDING',
    };

    db.insertSignal.run(signal);
    newSignals.push(signal);

    log.signal(
      `CONSENSUS: "${best.market_title?.slice(0,50)}" ${best.outcome} | ` +
      `${uniqueTraders.length} traders | avg price: ${(avgPrice*100).toFixed(1)}¢`
    );
  }

  return newSignals;
}

// ── TITLE CLUSTERING ──────────────────────────────────────────
// Groups trades from different market IDs that discuss the same event
// Uses fuzzy matching on market titles

function clusterByTitle(trades) {
  const groups   = new Map();
  const titled   = trades.filter(t => t.market_title && t.market_title.length > 5);
  const fuse     = new Fuse(titled, {
    keys:              ['market_title'],
    threshold:          0.3,  // 0=exact, 1=anything
    includeScore:       true,
    minMatchCharLength: 5,
  });

  const clustered = new Set();

  for(const trade of titled) {
    if(clustered.has(trade.id)) continue;
    const results = fuse.search(trade.market_title);
    const matches = results
      .filter(r => r.score < 0.3 && r.item.id !== trade.id)
      .map(r => r.item);

    if(matches.length > 0) {
      const clusterKey = `title::${trade.market_title}::${trade.outcome}`;
      const group      = [trade, ...matches.filter(m => m.outcome === trade.outcome)];
      groups.set(clusterKey, group);
      group.forEach(g => clustered.add(g.id));
    }
  }

  return groups;
}

module.exports = { detectSignals };
