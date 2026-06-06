// ============================================================
// POLYMARKET → KALSHI SIGNAL BOT
// Monitors top-50 Polymarket traders, detects consensus buys,
// matches to Kalshi markets, alerts via Discord + paper trades
// ============================================================

require('dotenv').config();
const cron       = require('node-cron');
const log        = require('./src/logger');
const db         = require('./src/db');
const config     = require('./src/config');
const poly       = require('./src/polymarket');
const signals    = require('./src/signals');
const kalshi     = require('./src/kalshi');
const alerts     = require('./src/alerts');
const { startDashboard } = require('./src/dashboard');

// ── STARTUP ───────────────────────────────────────────────────

async function startup() {
  console.log('\n╔══════════════════════════════════════════════════════════════╗');
  console.log('║  📊 POLYMARKET → KALSHI SIGNAL BOT                           ║');
  console.log('║  Alert + Paper Trade mode — real trading OFF                  ║');
  console.log('╚══════════════════════════════════════════════════════════════╝\n');

  log.info(`Real trading: ${config.trading.enableReal ? '⚠️ ENABLED' : '✅ DISABLED (paper only)'}`);
  log.info(`Signal window: ${config.signal.timeWindowMinutes} min | Min traders: ${config.signal.minTraders}`);
  log.info(`Kalshi threshold: ${config.signal.matchThreshold}/100 | Leaderboard size: ${config.signal.leaderboardSize}`);
  log.info(`Paper size: $${config.paper.minSizeUsd}-$${config.paper.maxSizeUsd} | Max positions: ${config.paper.maxOpenPositions}`);

  if(config.trading.enableReal) {
    log.warn('⚠️  REAL TRADING IS ENABLED — actual money will be spent on Kalshi!');
    log.warn('⚠️  Set ENABLE_REAL_TRADING=false in .env to disable');
  }

  // Validate Kalshi API key
  if(!config.kalshi.apiKey) {
    log.warn('No Kalshi API key — Kalshi matching will be skipped');
  }

  // Start dashboard
  startDashboard();

  // Initial leaderboard fetch
  log.info('Fetching initial leaderboard...');
  const traders = await poly.fetchLeaderboard();
  if(traders.length === 0) {
    log.warn('No traders fetched — check Polymarket API connectivity');
  } else {
    log.info(`Leaderboard ready: ${traders.length} traders`);
    await alerts.discord(
      `📊 **Polymarket → Kalshi Bot ONLINE**\n` +
      `━━━━━━━━━━━━━━━━━━━━\n` +
      `👥 Tracking **${traders.length}** top Polymarket traders\n` +
      `⏱  Signal window: **${config.signal.timeWindowMinutes} min**\n` +
      `🎯 Min traders for signal: **${config.signal.minTraders}**\n` +
      `🔗 Kalshi threshold: **${config.signal.matchThreshold}/100**\n` +
      `💸 Paper size: **$${config.paper.minSizeUsd}-$${config.paper.maxSizeUsd}**\n` +
      `🛑 Real trading: **${config.trading.enableReal ? '⚠️ ENABLED' : 'OFF'}**`
    );
  }

  // Initial trade poll
  log.info('Running initial trade poll...');
  await runPollCycle();
}

// ── MAIN POLL CYCLE ───────────────────────────────────────────

async function runPollCycle() {
  try {
    // 1. Fetch latest trades from all top-50 traders
    await poly.pollAllTraderTrades();

    // 2. Detect consensus signals
    const newSignals = await signals.detectSignals();
    log.info(`Signal scan complete — ${newSignals.length} new signals`);

    // 3. Process each new signal
    for(const signal of newSignals) {
      await processSignal(signal);
    }

  } catch(e) {
    log.error(`Poll cycle error: ${e.message}`);
  }
}

// ── PROCESS A SIGNAL ──────────────────────────────────────────

async function processSignal(signal) {
  log.signal(
    `Processing signal: "${signal.market_title?.slice(0,50)}" | ` +
    `${signal.outcome} | ${signal.trader_count} traders`
  );

  // Update daily signal count
  const today = new Date().toISOString().slice(0,10);
  db.upsertDailyStat.run({ date:today, pnl_usd:0, trades:0, wins:0, losses:0, signals:1 });

  // Find Kalshi match
  let kalshiMatch = null;

  if(config.kalshi.apiKey) {
    kalshiMatch = await kalshi.findKalshiMatch(signal);

    // Update signal with Kalshi result
    db.updateSignalKalshi.run({
      id:                signal.id,
      kalshi_ticker:     kalshiMatch?.ticker     || null,
      kalshi_title:      kalshiMatch?.title      || null,
      kalshi_confidence: kalshiMatch?.confidence || null,
      kalshi_price:      kalshiMatch?.yesPrice   || null,
      status:            kalshiMatch && !kalshiMatch.skipped ? 'MATCHED' : 'SKIPPED',
      skip_reason:       kalshiMatch?.skipReason || (kalshiMatch ? null : 'no_match'),
    });
  } else {
    db.updateSignalKalshi.run({
      id: signal.id,
      kalshi_ticker: null, kalshi_title: null,
      kalshi_confidence: null, kalshi_price: null,
      status: 'SKIPPED', skip_reason: 'no_kalshi_key',
    });
  }

  // Alert via Discord
  await alerts.alertSignal(signal, kalshiMatch);

  // Update status to ALERTED
  db.updateSignalKalshi.run({
    id: signal.id,
    kalshi_ticker:     kalshiMatch?.ticker     || null,
    kalshi_title:      kalshiMatch?.title      || null,
    kalshi_confidence: kalshiMatch?.confidence || null,
    kalshi_price:      kalshiMatch?.yesPrice   || null,
    status:            'ALERTED',
    skip_reason:       null,
  });
}

// ── CRON JOBS ─────────────────────────────────────────────────

function setupCronJobs() {
  // Poll trader trades every N minutes
  const pollMinutes = config.polling.tradesMinutes;
  log.info(`Setting up poll every ${pollMinutes} minutes`);

  cron.schedule(`*/${pollMinutes} * * * *`, async () => {
    log.poll('Cron: running trade poll cycle');
    await runPollCycle();
  });

  // Refresh leaderboard every N hours
  const leaderboardHours = config.polling.leaderboardHours;
  log.info(`Setting up leaderboard refresh every ${leaderboardHours} hours`);

  cron.schedule(`0 */${leaderboardHours} * * *`, async () => {
    log.poll('Cron: refreshing leaderboard');
    await poly.fetchLeaderboard();
  });

  // Health log every 30 minutes
  cron.schedule('*/30 * * * *', () => {
    const openTrades = db.getOpenPaperTrades.all();
    const traders    = db.getTopTraders.all(1);
    log.info(`Health: ${openTrades.length} open trades | ${traders.length>0?'traders loaded':'no traders'}`);
  });
}

// ── MAIN ──────────────────────────────────────────────────────

async function main() {
  await startup();
  setupCronJobs();
  log.info('Bot running — press Ctrl+C to stop');
}

process.on('SIGINT', async () => {
  log.info('Shutting down...');
  await alerts.discord('🔴 **Polymarket → Kalshi Bot OFFLINE**');
  process.exit(0);
});

process.on('unhandledRejection', e => log.error(`Unhandled: ${e.message}`));

main().catch(e => {
  log.error(`Fatal: ${e.message}`);
  process.exit(1);
});
