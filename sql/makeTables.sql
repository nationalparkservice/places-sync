CREATE TABLE IF NOT EXISTS public.log (
  process VARCHAR(40),
  key VARCHAR(40),
  hash VARCHAR(40),
  PRIMARY KEY (process, key)
);

CREATE TABLE IF NOT EXISTS public.checks (
  process VARCHAR(40),
  key VARCHAR(40),
  source VARCHAR(40),
  hash VARCHAR(40),
  check_idx INTEGER,
  check_date TEXT


