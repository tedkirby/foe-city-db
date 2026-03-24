import duckdb

DB_PATH = "/home/ted/foe/city-engine/data/city_engine.duckdb"
PROFILE = "TedMilitary"

con = duckdb.connect(DB_PATH)

# -----------------------------
# 1. Get attribute columns
# -----------------------------
attrs = con.execute(
    """
    SELECT attribute
    FROM config_weights
    WHERE profile = ?
      AND mode = 'attributes'
""",
    [PROFILE],
).fetchall()

attrs = [a[0] for a in attrs]

valid_cols = {r[0] for r in con.execute("DESCRIBE citycore").fetchall()}

missing = [a for a in attrs if a not in valid_cols]
if missing:
    print("⚠️ Missing columns:", missing)

attrs = [a for a in attrs if a in valid_cols]

if not attrs:
    raise Exception("No attributes found for profile")

col_list = ", ".join(attrs)

# -----------------------------
# 2. Build query
# -----------------------------
query = f"""
CREATE OR REPLACE VIEW building_scores AS
WITH attr_score AS (
  SELECT
    u.Building,
    SUM(u.value * w.weight) AS attr_score
  FROM citycore
  UNPIVOT (
    value FOR attribute IN ({col_list})
  ) u
  JOIN config_weights w
    ON u.attribute = w.attribute
  WHERE w.profile = '{PROFILE}'
    AND w.mode = 'attributes'
  GROUP BY u.Building
),
item_score AS (
  SELECT
    i.building,
    SUM(i.amount * w.weight) AS item_score
  FROM items_fragments i
  JOIN config_weights w
    ON i.item_name = w.attribute
  WHERE w.profile = '{PROFILE}'
    AND w.mode = 'items'
  GROUP BY i.building
)
SELECT
  c.Building,
  COALESCE(a.attr_score, 0) AS attr_score,
  COALESCE(i.item_score, 0) AS item_score,
  COALESCE(a.attr_score, 0) + COALESCE(i.item_score, 0) AS total_score
FROM citycore c
LEFT JOIN attr_score a ON c.Building = a.Building
LEFT JOIN item_score i ON c.Building = i.building;
"""

# -----------------------------
# 3. Execute
# -----------------------------
con.execute(query)

print(query)
print(f"✅ View created for profile: {PROFILE}")
print(f"Attributes used: {len(attrs)}")
