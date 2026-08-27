CREATE TABLE IF NOT EXISTS bars (
  ts INTEGER NOT NULL,
  source TEXT NOT NULL,
  btc REAL, eth REAL, sol REAL, bnb REAL, doge REAL, xrp REAL,
  PRIMARY KEY (source, ts)
) WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS idx_bars_ts ON bars(ts);
CREATE TABLE IF NOT EXISTS health (
  source TEXT PRIMARY KEY,
  last_ok INTEGER,
  last_err TEXT,
  updated INTEGER
);
CREATE TABLE IF NOT EXISTS meta (k TEXT PRIMARY KEY, v TEXT);
