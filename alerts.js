const { v4: uuidv4 } = require('uuid');
const fetch          = require('node-fetch');
const log            = require('./logger');
const db             = require('./db');
const config         = require('./config');

// ── DISCORD ───────────────────────────────────────────────────

async function discord(msg) {
  if(!config.discord.webhook) return;
  try {
    await fetch(config.discord.webhook, {
      method:  'POST',
      headers: { 'Content-Type': 'application/json' },
      body:    JSON.stringify({ content: msg.slice(0, 1990) }),
    });
  } catch(e) { log.error(`Discord: ${e.message}`); }
}

// ── SIGNAL ALERT ──────────────────────────────────────────────

async function alertSignal(signal, kalshiMatch) {
  const traders  = JSON.parse(signal.traders || '[]');
  const avgPct   = (signal.avg_price * 100).toFixed(1);
  const side     = signal.outcome === 'YES' ? 'YES' : 'NO';
  const kPrice   = kalshiMatch ? (kalshiMatch.yesPrice * 100).toFixed(1) : 'N/A';
  const confStr  = kalshiMatch ? `${kalshiMatch.confidence}/100` : 'N/A';

  const msg = [
    `🎯  **SIGNAL DETECTED**`,
    `━━━━━━━━━━━━━━━━━━━━`,
    `📊  **Polymarket:** ${signal.market_title?.slice(0,60)}`,
    `📈  **Outcome:** ${side} | Avg price: **${avgPct}¢**`,
    `👥  **Traders:** ${traders.length} top-50 traders agreed`,
    `⏱   **Window:** ${signal.window_minutes} minutes`,
    `━━━━━━━━━━━━━━━━━━━━`,
    kalshiMatch && !kalshiMatch.skipped
      ? [
          `🔗  **Kalshi Match:** ${kalshiMatch.title?.slice(0,60)}`,
          `🎯  **Confidence:** ${confStr}`,
          `💰  **Kalshi YES price:** ${kPrice}¢`,
          `📋  **Ticker:** \`${kalshiMatch.ticker}\``,
          ``,
          `📋  **PAPER TRADE RECOMMENDED** (not real money)`,
          `🛑  REAL TRADING: ${config.trading.enableReal ? '⚠️ ENABLED' : '✅ DISABLED'}`,
        ].join('\n')
      : `⏭  No strong Kalshi match found (threshold: ${config.signal.matchThreshold}/100)`,
    `━━━━━━━━━━━━━━━━━━━━`,
    `🆔  Signal ID: \`${signal.id.slice(0,8)}\``,
  ].join('\n');

  await discord(msg);
  log.signal(`Alert sent for signal ${signal.id.slice(0,8)}`);
}

// ── PAPER TRADE ───────────────────────────────────────────────

function openPaperTrade(signal, kalshiMatch) {
  if(!kalshiMatch || kalshiMatch.skipped) return null;

  // Risk checks
  const openTrades  = db.getOpenPaperTrades.all();
  if(openTrades.length >= config.paper.maxOpenPositions) {
    log.skip(`Max open positions (${config.paper.maxOpenPositions}) reached`);
    return null;
  }

  // Check daily loss limit
  const dailyPnl = db.getDailyPnl.get()?.total || 0;
  if(dailyPnl <= -config.paper.maxDailyLossUsd) {
    log.skip(`Daily loss limit reached: $${Math.abs(dailyPnl).toFixed(2)}`);
    return null;
  }

  // Check per-market exposure
  const marketExposure = openTrades
    .filter(t => t.kalshi_ticker === kalshiMatch.ticker)
    .reduce((s, t) => s + t.size_usd, 0);

  if(marketExposure >= config.paper.maxPerMarketUsd) {
    log.skip(`Max exposure for ${kalshiMatch.ticker}: $${marketExposure.toFixed(2)}`);
    return null;
  }

  // Determine size — scale with confidence
  const confRatio = kalshiMatch.confidence / 100;
  const sizeRange = config.paper.maxSizeUsd - config.paper.minSizeUsd;
  const sizeUsd   = config.paper.minSizeUsd + (sizeRange * confRatio);
  const roundSize = Math.round(sizeUsd * 100) / 100;

  const entryPrice = signal.outcome === 'YES'
    ? kalshiMatch.yesPrice
    : kalshiMatch.noPrice;

  const contracts = Math.floor((roundSize / entryPrice) * 100) / 100;

  const trade = {
    id:           uuidv4(),
    signal_id:    signal.id,
    kalshi_ticker: kalshiMatch.ticker,
    side:         signal.outcome,
    entry_price:  entryPrice,
    size_usd:     roundSize,
    contracts,
    opened_at:    Math.floor(Date.now()/1000),
  };

  db.insertPaperTrade.run(trade);

  log.paper(
    `Paper trade opened: ${kalshiMatch.ticker} ${signal.outcome} | ` +
    `$${roundSize} | ${contracts} contracts @ ${(entryPrice*100).toFixed(1)}¢`
  );

  discord([
    `📋  **PAPER TRADE OPENED**`,
    `━━━━━━━━━━━━━━━━━━━━`,
    `🎯  ${kalshiMatch.ticker} — **${signal.outcome}**`,
    `💸  Size: **$${roundSize}** | Contracts: **${contracts}**`,
    `💰  Entry price: **${(entryPrice*100).toFixed(1)}¢**`,
    `🎯  Confidence: **${kalshiMatch.confidence}/100**`,
    `🆔  Trade ID: \`${trade.id.slice(0,8)}\``,
  ].join('\n'));

  return trade;
}

// ── CLOSE PAPER TRADE ─────────────────────────────────────────

function closePaperTrade(tradeId, exitPrice, reason) {
  const trades = db.getOpenPaperTrades.all();
  const trade  = trades.find(t => t.id === tradeId);
  if(!trade) return null;

  const pnlUsd = (exitPrice - trade.entry_price) * trade.contracts * 100;
  const pnlPct = ((exitPrice - trade.entry_price) / trade.entry_price) * 100;

  db.updatePaperTrade.run({
    id:           tradeId,
    status:       'CLOSED',
    closed_at:    Math.floor(Date.now()/1000),
    exit_price:   exitPrice,
    pnl_usd:      pnlUsd,
    pnl_pct:      pnlPct,
    close_reason: reason,
  });

  // Update daily stats
  const today = new Date().toISOString().slice(0,10);
  db.upsertDailyStat.run({
    date:    today,
    pnl_usd: pnlUsd,
    trades:  1,
    wins:    pnlUsd >= 0 ? 1 : 0,
    losses:  pnlUsd < 0 ? 1 : 0,
    signals: 0,
  });

  const sign = pnlUsd >= 0 ? '+' : '';
  log.close(
    `Paper trade closed: ${trade.kalshi_ticker} | ` +
    `${sign}$${pnlUsd.toFixed(2)} (${sign}${pnlPct.toFixed(1)}%) | ${reason}`
  );

  discord([
    `💰  **PAPER TRADE CLOSED**`,
    `━━━━━━━━━━━━━━━━━━━━`,
    `🎯  ${trade.kalshi_ticker} — **${trade.side}**`,
    `📈  Entry: **${(trade.entry_price*100).toFixed(1)}¢** → Exit: **${(exitPrice*100).toFixed(1)}¢**`,
    `${pnlUsd >= 0 ? '📈' : '📉'}  PnL: **${sign}$${pnlUsd.toFixed(2)}** (${sign}${pnlPct.toFixed(1)}%)`,
    `📋  Reason: **${reason}**`,
  ].join('\n'));

  return { pnlUsd, pnlPct };
}

module.exports = { discord, alertSignal, openPaperTrade, closePaperTrade };
