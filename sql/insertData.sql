INSERT INTO master (
  key,
  process,
  source,
  hash,
  data,
  last_update,
  removed
) VALUES (
  {{key}},
  {{process}},
  {{source}},
  {{hash}},
  {{data}},
  {{last_update}},
  {{removed}}
);
