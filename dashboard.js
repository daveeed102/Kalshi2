const express = require('express');
const db      = require('./db');
const log     = require('./logger');
const config  = require('./config');

const app = express();

// ── HTML DASHBOARD ────────────────────────────────────────────

const html = () => {
  const signals     = db.getRecentSignals.all(20);
  const paperTrades = db.getAllPaperTrades.all(20);
  const traders     = db.getTopTraders.all(10);
  const stats       = db.getDailyStats.all();
  const openTrades  = db.getOpenPaperTrades.all();

  const totalPnl   = paperTrades.filter(t=>t.status==='CLOSED').reduce((s,t)=>s+(t.pnl_usd||0),0);
  const wins       = paperTrades.filter(t=>t.status==='CLOSED'&&(t.pnl_usd||0)>=0).length;
  const losses     = paperTrades.filter(t=>t.status==='CLOSED'&&(t.pnl_usd||0)<0).length;
  const wr         = wins+losses>0 ? ((wins/(wins+losses))*100).toFixed(0) : '0';

  const signalRows = signals.map(s => `
    <tr>
      <td>${new Date(s.detected_at*1000).toLocaleTimeString()}</td>
      <td>${(s.market_title||'').slice(0,45)}</td>
      <td><b>${s.outcome}</b></td>
      <td>${s.trader_count}</td>
      <td>${((s.avg_price||0)*100).toFixed(1)}¢</td>
      <td>${s.kalshi_ticker||'—'}</td>
      <td>${s.kalshi_confidence ? s.kalshi_confidence.toFixed(0)+'/100' : '—'}</td>
      <td><span class="badge ${s.status}">${s.status}</span></td>
    </tr>`).join('');

  const tradeRows = paperTrades.map(t => {
    const pnl  = t.pnl_usd || 0;
    const sign = pnl >= 0 ? '+' : '';
    return `
    <tr>
      <td>${new Date(t.opened_at*1000).toLocaleTimeString()}</td>
      <td>${t.kalshi_ticker}</td>
      <td>${t.side}</td>
      <td>$${t.size_usd?.toFixed(2)||'0'}</td>
      <td>${((t.entry_price||0)*100).toFixed(1)}¢</td>
      <td>${t.exit_price ? ((t.exit_price)*100).toFixed(1)+'¢' : '—'}</td>
      <td class="${pnl>=0?'pos':'neg'}">${t.status==='CLOSED'?sign+'$'+pnl.toFixed(2):'open'}</td>
      <td><span class="badge ${t.status}">${t.status}</span></td>
    </tr>`;
  }).join('');

  const traderRows = traders.map(t => `
    <tr>
      <td>#${t.rank}</td>
      <td><code>${(t.id||'').slice(0,16)}...</code></td>
      <td>$${(t.profit_30d||0).toFixed(0)}</td>
      <td>$${(t.volume_30d||0).toFixed(0)}</td>
    </tr>`).join('');

  return `<!DOCTYPE html>
<html>
<head>
  <title>Polymarket → Kalshi Signal Bot</title>
  <meta http-equiv="refresh" content="30">
  <style>
    * { box-sizing: border-box; margin: 0; padding: 0; }
    body { font-family: 'Segoe UI', sans-serif; background: #0f1117; color: #e2e8f0; padding: 20px; }
    h1 { color: #a78bfa; margin-bottom: 4px; font-size: 1.4rem; }
    h2 { color: #7c3aed; font-size: 1rem; margin: 20px 0 8px; text-transform: uppercase; letter-spacing: 1px; }
    .meta { color: #64748b; font-size: 0.8rem; margin-bottom: 20px; }
    .cards { display: flex; gap: 12px; flex-wrap: wrap; margin-bottom: 20px; }
    .card { background: #1e1e2e; border: 1px solid #2d2d3d; border-radius: 8px; padding: 16px 20px; min-width: 140px; }
    .card .val { font-size: 1.6rem; font-weight: bold; color: #a78bfa; }
    .card .label { font-size: 0.75rem; color: #64748b; margin-top: 2px; }
    .pos { color: #34d399; }
    .neg { color: #f87171; }
    table { width: 100%; border-collapse: collapse; margin-bottom: 24px; font-size: 0.82rem; }
    th { background: #1a1a2e; color: #7c3aed; text-align: left; padding: 8px 10px; border-bottom: 1px solid #2d2d3d; }
    td { padding: 7px 10px; border-bottom: 1px solid #1a1a2e; }
    tr:hover td { background: #1e1e2e; }
    .badge { padding: 2px 8px; border-radius: 12px; font-size: 0.7rem; font-weight: bold; }
    .badge.PENDING  { background: #374151; color: #9ca3af; }
    .badge.MATCHED  { background: #1e3a5f; color: #60a5fa; }
    .badge.ALERTED  { background: #3b1f6b; color: #c4b5fd; }
    .badge.SKIPPED  { background: #2d1b1b; color: #ef4444; }
    .badge.OPEN     { background: #1f3a1f; color: #34d399; }
    .badge.CLOSED   { background: #1e293b; color: #94a3b8; }
    .kill { background: #7c3aed; color: white; padding: 4px 12px; border-radius: 4px; font-size: 0.75rem; }
    .kill.on { background: #dc2626; }
    code { font-size: 0.78rem; color: #94a3b8; }
  </style>
</head>
<body>
  <h1>📊 Polymarket → Kalshi Signal Bot</h1>
  <p class="meta">
    Auto-refreshes every 30s · Last updated: ${new Date().toLocaleString()} ·
    Real trading: <span class="kill ${config.trading.enableReal?'on':''}">${config.trading.enableReal?'⚠️ ENABLED':'✅ OFF'}</span>
  </p>

  <div class="cards">
    <div class="card"><div class="val">${signals.length}</div><div class="label">Signals (recent)</div></div>
    <div class="card"><div class="val">${openTrades.length}</div><div class="label">Open Positions</div></div>
    <div class="card"><div class="val ${totalPnl>=0?'pos':'neg'}">${totalPnl>=0?'+':''}$${totalPnl.toFixed(2)}</div><div class="label">Total Paper PnL</div></div>
    <div class="card"><div class="val">${wr}%</div><div class="label">Win Rate (${wins}W/${losses}L)</div></div>
    <div class="card"><div class="val">${traders.length}</div><div class="label">Top Traders Tracked</div></div>
  </div>

  <h2>🎯 Recent Signals</h2>
  <table>
    <tr><th>Time</th><th>Market</th><th>Side</th><th>Traders</th><th>Avg Price</th><th>Kalshi</th><th>Confidence</th><th>Status</th></tr>
    ${signalRows || '<tr><td colspan="8" style="color:#64748b;text-align:center">No signals yet</td></tr>'}
  </table>

  <h2>📋 Paper Trades</h2>
  <table>
    <tr><th>Opened</th><th>Ticker</th><th>Side</th><th>Size</th><th>Entry</th><th>Exit</th><th>PnL</th><th>Status</th></tr>
    ${tradeRows || '<tr><td colspan="8" style="color:#64748b;text-align:center">No paper trades yet</td></tr>'}
  </table>

  <h2>👤 Top Traders</h2>
  <table>
    <tr><th>Rank</th><th>Address</th><th>30D Profit</th><th>30D Volume</th></tr>
    ${traderRows || '<tr><td colspan="4" style="color:#64748b;text-align:center">No traders yet — run leaderboard fetch</td></tr>'}
  </table>
</body>
</html>`;
};

// ── API ENDPOINTS ─────────────────────────────────────────────

app.get('/', (req, res) => res.send(html()));

app.get('/api/signals', (req, res) => {
  res.json(db.getRecentSignals.all(50));
});

app.get('/api/trades', (req, res) => {
  res.json(db.getAllPaperTrades.all(50));
});

app.get('/api/stats', (req, res) => {
  res.json(db.getDailyStats.all());
});

app.get('/api/traders', (req, res) => {
  res.json(db.getTopTraders.all(50));
});

function startDashboard() {
  const port = config.dashboard.port;
  app.listen(port, () => {
    log.dash(`Dashboard running at http://localhost:${port}`);
  });
}

module.exports = { startDashboard };
