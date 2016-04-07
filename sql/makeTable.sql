CREATE TABLE IF NOT EXISTS master (
  key TEXT,
  process TEXT,
  source TEXT,
  hash TEXT,
  last_update NUMERIC,
  data BLOB,
  removed NUMERIC,
  PRIMARY KEY (process, source, key)
);
