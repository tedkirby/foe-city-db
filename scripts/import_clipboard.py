import duckdb
import sys

DB_PATH = "~/foe/city-engine/data/city_engine.duckdb"

TABLE_CONFIG = {
    "city": {"col": "count", "type": int, "min": 1, "aggregate": False},
    "inventory": {"col": "count", "type": int, "min": 1, "aggregate": True},
    "max_build": {"col": "count", "type": int, "min": 0, "aggregate": False},
    "sets": {"col": "sets", "type": float, "min": 0, "aggregate": False},
}

DEFAULT_USER = "ted"


# -----------------------------
# Helpers
# -----------------------------


def resolve_table(name):
    name = name.lower()
    matches = [t for t in TABLE_CONFIG if t.startswith(name)]

    if len(matches) == 1:
        return matches[0]

    if len(matches) == 0:
        raise ValueError(f"Unknown table: {name}")

    raise ValueError(f"Ambiguous table name: {name} → {matches}")


def parse_clipboard(text):
    lines = [l.strip() for l in text.strip().split("\n") if l.strip()]
    return [line.split("\t") for line in lines]


def validate(value, cfg):
    if value == "" or value is None:
        raise ValueError("Empty value not allowed")

    try:
        v = cfg["type"](value)
    except Exception:
        raise ValueError(f"Invalid number: {value}")

    if v < cfg["min"]:
        raise ValueError(f"Value {v} < min {cfg['min']}")

    return v


def aggregate_rows(rows):
    agg = {}

    for building, value in rows:
        if building not in agg:
            agg[building] = value
        else:
            agg[building] += value

    return [[b, v] for b, v in agg.items()]


# -----------------------------
# Main
# -----------------------------


def main():
    if len(sys.argv) < 2:
        print("Usage: python import_clipboard.py <table> [user]")
        sys.exit(1)

    table_input = sys.argv[1]
    user = sys.argv[2] if len(sys.argv) > 2 else DEFAULT_USER

    try:
        table = resolve_table(table_input)
    except Exception as e:
        print(e)
        sys.exit(1)

    cfg = TABLE_CONFIG[table]
    value_col = cfg["col"]

    print(f"\nTable: {table} | User: {user}")
    print("Paste data, then Ctrl+D:\n")

    text = sys.stdin.read()
    rows = parse_clipboard(text)

    if not rows or len(rows[0]) < 2:
        print("Invalid input format")
        sys.exit(1)

    data = rows

    validated = []
    errors = []

    for i, row in enumerate(data, start=2):
        building = row[0].strip()

        try:
            value = validate(row[1], cfg)
            validated.append([building, value])
        except Exception as e:
            errors.append(f"Row {i} ({building}): {e}")

    if errors:
        print("\n❌ Validation errors:")
        for e in errors[:10]:
            print(e)

        print(f"\n{len(errors)} total errors. Aborting.")
        sys.exit(1)

    # Aggregate if needed
    if cfg["aggregate"]:
        aggregated = aggregate_rows(validated)
    else:
        aggregated = validated

    cleaned = [[user, b, v] for b, v in aggregated]

    con = duckdb.connect(DB_PATH)

    # Delete existing rows for this user
    con.execute(f"DELETE FROM {table} WHERE user_id = ?", [user])

    # Insert new data
    con.executemany(
        f"INSERT INTO {table} (user_id, building, {value_col}) VALUES (?, ?, ?)",
        cleaned,
    )

    print(f"\n✅ Loaded {len(cleaned)} rows into {table} for [{user}]")


if __name__ == "__main__":
    main()
