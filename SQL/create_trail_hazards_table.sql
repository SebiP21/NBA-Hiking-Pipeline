CREATE TABLE IF NOT EXISTS trail_hazards_danger_raw (
  "Name"          TEXT PRIMARY KEY,
  "Rattlesnakes"  TEXT,
  "Ticks"         TEXT,
  "Posionivy"     TEXT,   -- (spelling kept to match CSV header)
  "Falling"       TEXT
);
