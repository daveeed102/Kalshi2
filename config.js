require('dotenv').config();

module.exports = {
  kalshi: {
    apiKey:    process.env.KALSHI_API_KEY    || '',
    apiSecret: process.env.KALSHI_API_SECRET || '',
    baseUrl:   process.env.KALSHI_BASE_URL   || 'https://trading-api.kalshi.com',
  },
  discord: {
    webhook: process.env.DISCORD_WEBHOOK_URL || '',
  },
  signal: {
    timeWindowMinutes:  parseInt(process.env.TIME_WINDOW_MINUTES)    || 15,
    minTraders:         parseInt(process.env.MIN_TRADERS_FOR_SIGNAL)  || 2,
    matchThreshold:     parseInt(process.env.KALSHI_MATCH_THRESHOLD)  || 70,
    leaderboardSize:    parseInt(process.env.LEADERBOARD_SIZE)        || 50,
  },
  paper: {
    minSizeUsd:         parseFloat(process.env.PAPER_TRADE_SIZE_MIN_USD)       || 3,
    maxSizeUsd:         parseFloat(process.env.PAPER_TRADE_SIZE_MAX_USD)       || 5,
    maxOpenPositions:   parseInt(process.env.MAX_OPEN_POSITIONS)               || 10,
    maxDailyLossUsd:    parseFloat(process.env.MAX_DAILY_LOSS_USD)             || 50,
    maxPerMarketUsd:    parseFloat(process.env.MAX_EXPOSURE_PER_MARKET_USD)    || 20,
    maxPerCategoryUsd:  parseFloat(process.env.MAX_EXPOSURE_PER_CATEGORY_USD)  || 50,
    priceCap:           parseFloat(process.env.PRICE_CAP)                      || 0.90,
  },
  trading: {
    enableReal: process.env.ENABLE_REAL_TRADING === 'true', // KILL SWITCH
  },
  polling: {
    tradesMinutes:      parseInt(process.env.POLL_TRADES_MINUTES)      || 5,
    leaderboardHours:   parseInt(process.env.POLL_LEADERBOARD_HOURS)   || 6,
  },
  dashboard: {
    port: parseInt(process.env.DASHBOARD_PORT) || 3000,
  },
};
