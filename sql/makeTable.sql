CREATE TABLE IF NOT EXISTS master (
  key TEXT,
  process TEXT,
  source TEXT,
  hash TEXT,
  last_update numeric,
  removed numeric,
  PRIMARY KEY (process, source, key)
);
