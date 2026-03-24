-- user-scoped tables

CREATE TABLE city (
  user_id TEXT,
  building TEXT,
  count INTEGER
);

CREATE TABLE inventory (
  user_id TEXT,
  building TEXT,
  count INTEGER
);

CREATE TABLE max_build (
  user_id TEXT,
  building TEXT,
  count INTEGER
);

CREATE TABLE sets (
  user_id TEXT,
  building TEXT,
  sets DOUBLE
);

-- current user context
CREATE TABLE IF NOT EXISTS current_user (
  user_id VARCHAR
);
