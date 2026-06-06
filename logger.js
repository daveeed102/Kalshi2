const fs   = require('fs');
const path = require('path');

const LOG_DIR  = path.join(__dirname, '..', 'logs');
const LOG_FILE = path.join(LOG_DIR, 'signals.log');
fs.mkdirSync(LOG_DIR, { recursive: true });

function ts() {
  return new Date().toISOString();
}

const icons = {
  INFO:    '📡',
  SIGNAL:  '🎯',
  MATCH:   '🔗',
  SKIP:    '⏭',
  PAPER:   '📋',
  CLOSE:   '💰',
  ERROR:   '❌',
  WARN:    '⚠️',
  TRADER:  '👤',
  POLL:    '🔄',
  DASH:    '📊',
};

function write(level, msg, data={}) {
  const icon    = icons[level] || '📋';
  const dataStr = Object.keys(data).length ? ' ' + JSON.stringify(data) : '';
  const line    = `[${ts()}] ${icon} [${level}] ${msg}${dataStr}`;
  console.log(line);
  fs.appendFileSync(LOG_FILE, line + '\n');
}

module.exports = {
  info:   (msg, d={}) => write('INFO',   msg, d),
  signal: (msg, d={}) => write('SIGNAL', msg, d),
  match:  (msg, d={}) => write('MATCH',  msg, d),
  skip:   (msg, d={}) => write('SKIP',   msg, d),
  paper:  (msg, d={}) => write('PAPER',  msg, d),
  close:  (msg, d={}) => write('CLOSE',  msg, d),
  error:  (msg, d={}) => write('ERROR',  msg, d),
  warn:   (msg, d={}) => write('WARN',   msg, d),
  trader: (msg, d={}) => write('TRADER', msg, d),
  poll:   (msg, d={}) => write('POLL',   msg, d),
  dash:   (msg, d={}) => write('DASH',   msg, d),
};
