const fetch  = require('node-fetch');
const Fuse   = require('fuse.js');
const log    = require('./logger');
const config = require('./config');

const BASE = config.kalshi.baseUrl;

// ── KALSHI AUTH ───────────────────────────────────────────────

function authHeaders() {
  return {
    'Content-Type':  'application/json',
    'Accept':        'application/json',
    'Authorization': `Bearer ${config.kalshi.apiKey}`,
  };
}

// ── FETCH KALSHI MARKETS ──────────────────────────────────────

async function fetchKalshiMarkets(query='', limit=100) {
  try {
    const params = new URLSearchParams({
      limit:  String(limit),
      status: 'open',
    });
    if(query) params.set('search', query);

    const r = await fetch(`${BASE}/trade-api/v2/markets?${params}`, {
      headers: authHeaders(),
    });

    if(!r.ok) {
      log.warn(`Kalshi markets ${r.status}: ${await r.text()}`);
      return [];
    }

    const data = await r.json();
    return data.markets || [];
  } catch(e) {
    log.error(`fetchKalshiMarkets: ${e.message}`);
    return [];
  }
}

async function fetchKalshiMarket(ticker) {
  try {
    const r = await fetch(`${BASE}/trade-api/v2/markets/${ticker}`, {
      headers: authHeaders(),
    });
    if(!r.ok) return null;
    const data = await r.json();
    return data.market || null;
  } catch(e) { return null; }
}

// ── FUZZY MATCH ───────────────────────────────────────────────
// Scores how well a Polymarket signal matches a Kalshi market
// Returns 0-100, threshold defined in config

function scoreMatch(polyTitle, polyOutcome, kalshiMarket) {
  let score = 0;

  const kTitle    = (kalshiMarket.title || '').toLowerCase();
  const kSubtitle = (kalshiMarket.subtitle || '').toLowerCase();
  const pTitle    = (polyTitle || '').toLowerCase();

  // ── Title similarity (0-40 pts) ──────────────────────────
  const fuse    = new Fuse([{ t: kTitle }], { keys:['t'], threshold:0.4, includeScore:true });
  const results = fuse.search(pTitle);
  if(results.length > 0) {
    const similarity = 1 - (results[0].score || 1);
    score += Math.round(similarity * 40);
  }

  // Keyword overlap bonus
  const polyWords   = pTitle.split(/\W+/).filter(w => w.length > 3);
  const kalshiWords = (kTitle + ' ' + kSubtitle).split(/\W+/).filter(w => w.length > 3);
  const overlap     = polyWords.filter(w => kalshiWords.includes(w)).length;
  const overlapPct  = polyWords.length > 0 ? overlap / polyWords.length : 0;
  score += Math.round(overlapPct * 20); // up to 20 extra pts

  // ── Category match (0-20 pts) ─────────────────────────────
  const CATEGORIES = {
    politics:  ['election', 'president', 'vote', 'congress', 'senate', 'trump', 'biden', 'harris'],
    crypto:    ['bitcoin', 'btc', 'eth', 'ethereum', 'crypto', 'sol', 'solana', 'price'],
    sports:    ['nfl', 'nba', 'mlb', 'nhl', 'world cup', 'super bowl', 'championship'],
    finance:   ['fed', 'interest rate', 'inflation', 'gdp', 'recession', 'market'],
    ai:        ['openai', 'gpt', 'claude', 'ai', 'artificial intelligence'],
  };

  let polyCategory   = null;
  let kalshiCategory = null;

  for(const [cat, words] of Object.entries(CATEGORIES)) {
    if(words.some(w => pTitle.includes(w)))                       polyCategory   = cat;
    if(words.some(w => kTitle.includes(w) || kSubtitle.includes(w))) kalshiCategory = cat;
  }

  if(polyCategory && kalshiCategory && polyCategory === kalshiCategory) score += 20;
  else if(polyCategory && kalshiCategory) score += 5; // different but both categorized

  // ── Outcome side match (0-20 pts) ────────────────────────
  // Kalshi YES = buy YES, NO = buy NO
  // If Poly outcome is YES and Kalshi market is a YES/NO market → match
  if(kalshiMarket.yes_bid || kalshiMarket.yes_ask) {
    // Binary market — both sides available
    score += 20;
  }

  // Cap at 100
  return Math.min(score, 100);
}

// ── FIND BEST KALSHI MATCH ────────────────────────────────────

async function findKalshiMatch(signal) {
  const polyTitle   = signal.market_title || '';
  const polyOutcome = signal.outcome || 'YES';

  if(!polyTitle) return null;

  // Extract key terms for search
  const keywords = polyTitle
    .replace(/[^a-zA-Z0-9 ]/g, ' ')
    .split(' ')
    .filter(w => w.length > 3)
    .slice(0, 3)
    .join(' ');

  log.match(`Searching Kalshi for: "${keywords}"`);

  const markets = await fetchKalshiMarkets(keywords, 50);
  if(!markets.length) {
    log.skip(`No Kalshi markets found for "${keywords}"`);
    return null;
  }

  // Score all matches
  const scored = markets
    .map(m => ({
      market:     m,
      confidence: scoreMatch(polyTitle, polyOutcome, m),
    }))
    .sort((a, b) => b.confidence - a.confidence);

  const best = scored[0];

  log.match(
    `Best Kalshi match: "${best.market.title?.slice(0,50)}" | ` +
    `confidence: ${best.confidence}/100 | ticker: ${best.market.ticker}`
  );

  if(best.confidence < config.signal.matchThreshold) {
    log.skip(`Confidence ${best.confidence} < threshold ${config.signal.matchThreshold} — no match`);
    return null;
  }

  // Get current price
  const detail    = await fetchKalshiMarket(best.market.ticker);
  const yesPrice  = detail?.yes_ask || detail?.last_price || 0;

  // Check price cap
  if(yesPrice > config.paper.priceCap) {
    log.skip(`Kalshi price ${yesPrice} > cap ${config.paper.priceCap}`);
    return { ...best, yesPrice, skipped: true, skipReason: 'price_cap' };
  }

  return {
    ticker:     best.market.ticker,
    title:      best.market.title,
    confidence: best.confidence,
    yesPrice,
    noPrice:    detail?.no_ask || (1 - yesPrice),
    market:     detail || best.market,
    skipped:    false,
  };
}

module.exports = {
  fetchKalshiMarkets,
  fetchKalshiMarket,
  findKalshiMatch,
  scoreMatch,
};
