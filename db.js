const Database = require('better-sqlite3');
const path     = require('path');
const fs       = require('fs');

const DB_PATH  = path.join(__dirname, '..', 'data', 'bot.db');
fs.mkdirSync(path.dirname(DB_PATH), { recursive: true });

const db = new Database(DB_PATH);
db.pragma('journal_mode = WAL');
db.pragma('foreign_keys = ON');

// ── SCHEMA ────────────────────────────────────────────────────

db.exec(`
  CREATE TABLE IF NOT EXISTS traders (
    id          TEXT PRIMARY KEY,
    rank        INTEGER,
    profit_30d  REAL,
    volume_30d  REAL,
    fetched_at  INTEGER
  );

  CREATE TABLE IF NOT EXISTS trades (
    id             TEXT PRIMARY KEY,
    trader_address TEXT NOT NULL,
    market_id      TEXT,
    market_title   TEXT,
    outcome        TEXT,
    side           TEXT,
    price          REAL,
    size           REAL,
    usd_value      REAL,
    timestamp      INTEGER,
    created_at     INTEGER DEFAULT (strftime('%s','now'))
  );

  CREATE INDEX IF NOT EXISTS idx_trades_trader   ON trades(trader_address);
  CREATE INDEX IF NOT EXISTS idx_trades_market   ON trades(market_id);
  CREATE INDEX IF NOT EXISTS idx_trades_timestamp ON trades(timestamp);

  CREATE TABLE IF NOT EXISTS signals (
    id                TEXT PRIMARY KEY,
    market_id         TEXT,
    market_title      TEXT,
    outcome           TEXT,
    traders           TEXT,
    trader_count      INTEGER,
    avg_price         REAL,
    window_minutes    INTEGER,
    detected_at       INTEGER,
    kalshi_ticker     TEXT,
    kalshi_title      TEXT,
    kalshi_confidence REAL,
    kalshi_price      REAL,
    status            TEXT DEFAULT 'PENDING',
    skip_reason       TEXT
  );

  CREATE TABLE IF NOT EXISTS paper_trades (
    id           TEXT PRIMARY KEY,
    signal_id    TEXT,
    kalshi_ticker TEXT,
    side         TEXT,
    entry_price  REAL,
    size_usd     REAL,
    contracts    INTEGER,
    status       TEXT DEFAULT 'OPEN',
    opened_at    INTEGER,
    closed_at    INTEGER,
    exit_price   REAL,
    pnl_usd      REAL,
    pnl_pct      REAL,
    close_reason TEXT,
    FOREIGN KEY (signal_id) REFERENCES signals(id)
  );

  CREATE TABLE IF NOT EXISTS daily_stats (
    date        TEXT PRIMARY KEY,
    pnl_usd     REAL DEFAULT 0,
    trades      INTEGER DEFAULT 0,
    wins        INTEGER DEFAULT 0,
    losses      INTEGER DEFAULT 0,
    signals     INTEGER DEFAULT 0
  );
`);

// ── TRADERS ───────────────────────────────────────────────────

const upsertTrader = db.prepare(`
  INSERT OR REPLACE INTO traders (id, rank, profit_30d, volume_30d, fetched_at)
  VALUES (@id, @rank, @profit_30d, @volume_30d, @fetched_at)
`);

const getTopTraders = db.prepare(`
  SELECT * FROM traders ORDER BY rank ASC LIMIT ?
`);

// ── TRADES ────────────────────────────────────────────────────

const upsertTrade = db.prepare(`
  INSERT OR IGNORE INTO trades
    (id, trader_address, market_id, market_title, outcome, side, price, size, usd_value, timestamp)
  VALUES
    (@id, @trader_address, @market_id, @market_title, @outcome, @side, @price, @size, @usd_value, @timestamp)
`);

const getRecentBuys = db.prepare(`
  SELECT * FROM trades
  WHERE side = 'BUY'
    AND timestamp > ?
  ORDER BY timestamp DESC
`);

const getTradesByMarket = db.prepare(`
  SELECT * FROM trades
  WHERE market_id = ?
    AND side = 'BUY'
    AND timestamp > ?
`);

// ── SIGNALS ───────────────────────────────────────────────────

const insertSignal = db.prepare(`
  INSERT OR IGNORE INTO signals
    (id, market_id, market_title, outcome, traders, trader_count,
     avg_price, window_minutes, detected_at, status)
  VALUES
    (@id, @market_id, @market_title, @outcome, @traders, @trader_count,
     @avg_price, @window_minutes, @detected_at, @status)
`);

const updateSignalKalshi = db.prepare(`
  UPDATE signals SET
    kalshi_ticker     = @kalshi_ticker,
    kalshi_title      = @kalshi_title,
    kalshi_confidence = @kalshi_confidence,
    kalshi_price      = @kalshi_price,
    status            = @status,
    skip_reason       = @skip_reason
  WHERE id = @id
`);

const getSignal = db.prepare(`SELECT * FROM signals WHERE id = ?`);

const getRecentSignals = db.prepare(`
  SELECT * FROM signals ORDER BY detected_at DESC LIMIT ?
`);

const signalExistsForMarket = db.prepare(`
  SELECT id FROM signals
  WHERE market_id = ?
    AND outcome = ?
    AND detected_at > ?
  LIMIT 1
`);

// ── PAPER TRADES ──────────────────────────────────────────────

const insertPaperTrade = db.prepare(`
  INSERT INTO paper_trades
    (id, signal_id, kalshi_ticker, side, entry_price, size_usd, contracts, opened_at)
  VALUES
    (@id, @signal_id, @kalshi_ticker, @side, @entry_price, @size_usd, @contracts, @opened_at)
`);

const updatePaperTrade = db.prepare(`
  UPDATE paper_trades SET
    status = @status, closed_at = @closed_at,
    exit_price = @exit_price, pnl_usd = @pnl_usd,
    pnl_pct = @pnl_pct, close_reason = @close_reason
  WHERE id = @id
`);

const getOpenPaperTrades = db.prepare(`
  SELECT * FROM paper_trades WHERE status = 'OPEN'
`);

const getAllPaperTrades = db.prepare(`
  SELECT * FROM paper_trades ORDER BY opened_at DESC LIMIT ?
`);

const getDailyPnl = db.prepare(`
  SELECT COALESCE(SUM(pnl_usd), 0) as total
  FROM paper_trades
  WHERE date(closed_at, 'unixepoch') = date('now')
    AND status = 'CLOSED'
`);

// ── DAILY STATS ───────────────────────────────────────────────

const upsertDailyStat = db.prepare(`
  INSERT INTO daily_stats (date, pnl_usd, trades, wins, losses, signals)
  VALUES (@date, @pnl_usd, @trades, @wins, @losses, @signals)
  ON CONFLICT(date) DO UPDATE SET
    pnl_usd  = pnl_usd  + excluded.pnl_usd,
    trades   = trades   + excluded.trades,
    wins     = wins     + excluded.wins,
    losses   = losses   + excluded.losses,
    signals  = signals  + excluded.signals
`);

const getDailyStats = db.prepare(`
  SELECT * FROM daily_stats ORDER BY date DESC LIMIT 30
`);

// ── HELPERS ───────────────────────────────────────────────────

function bulkUpsertTraders(traderList) {
  const tx = db.transaction((list) => {
    for(const t of list) upsertTrader.run(t);
  });
  tx(traderList);
}

function bulkUpsertTrades(tradeList) {
  const tx = db.transaction((list) => {
    for(const t of list) upsertTrade.run(t);
  });
  tx(tradeList);
}

module.exports = {
  db,
  bulkUpsertTraders,
  bulkUpsertTrades,
  getTopTraders,
  upsertTrade,
  getRecentBuys,
  getTradesByMarket,
  insertSignal,
  updateSignalKalshi,
  getSignal,
  getRecentSignals,
  signalExistsForMarket,
  insertPaperTrade,
  updatePaperTrade,
  getOpenPaperTrades,
  getAllPaperTrades,
  getDailyPnl,
  upsertDailyStat,
  getDailyStats,
};
