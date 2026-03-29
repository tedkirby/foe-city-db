from flask import Flask, request
from app.db import get_connection
import re

app = Flask(__name__)

# -----------------------------
# Type inference
# -----------------------------


def infer_type(values):
    has_float = False
    has_int = False

    for v in values:
        if v is None:
            continue

        # already numeric (after normalization)
        if isinstance(v, int):
            has_int = True
            continue

        if isinstance(v, float):
            has_float = True
            continue

        # string fallback
        try:
            if "." in str(v):
                float(v)
                has_float = True
            else:
                int(v)
                has_int = True
        except ValueError:
            return "TEXT"

    if has_float:
        return "DOUBLE"

    if has_int:
        return "INTEGER"

    return "TEXT"


FORCE_DOUBLE_PREFIXES = ("Multiplier",)


def infer_schema(headers, rows):
    cols = list(zip(*rows))
    types = []

    for i, col in enumerate(cols):
        name = headers[i]

        # 🔥 FORCE rule
        if name.startswith(FORCE_DOUBLE_PREFIXES):
            types.append("DOUBLE")
            continue

        types.append(infer_type(col))

    return list(zip(headers, types))


def clean_col(name):
    return str(name).strip().replace('"', "").replace("/", "_").replace(" ", "_")


def clean_number(v):
    if v is None:
        return None

    s = str(v).strip()

    if s in ("", "-", "—", "N/A"):
        return None

    return s  # let DuckDB cast later


def normalize_data(rows):
    return [[clean_number(v) for v in row] for row in rows]


def populate_items_fragments(con, headers, data):
    """
    Build items_fragments table from Items_Fragments column.
    Expects:
      - con: duckdb connection
      - headers: cleaned column names
      - data: normalized rows (same as used for citycore)
    """

    # -----------------------------
    # Find required columns
    # -----------------------------
    try:
        items_idx = headers.index("Items_Fragments")
        building_idx = headers.index("Building")
    except ValueError:
        print("⚠️ Items_Fragments or Building column not found")
        return

    # print("HEADERS:", headers)
    # print("Items idx:", items_idx, "Building idx:", building_idx)

    # for i, row in enumerate(data[:5]):
    #     print("ROW SAMPLE:", row[items_idx])

    # -----------------------------
    # Parser
    # -----------------------------
    def parse_items(cell):
        if not cell:
            return []

        rows = str(cell).split("\n")
        result = []

        for r in rows:
            r = r.strip()
            if not r:
                continue

            # matches: "Name – 5" or "Name – 5/30"
            match = re.match(r"(.*) – (-?[0-9.]+)(?:/([0-9]+))?", r)
            if not match:
                print("NO MATCH:", r)
                continue

            name = match.group(1).strip()
            value = float(match.group(2))
            divisor = float(match.group(3)) if match.group(3) else 1

            result.append((name, value / divisor))

        return result

    # -----------------------------
    # Build rows
    # -----------------------------
    parsed_rows = []

    for row in data:
        building = row[building_idx]
        items = row[items_idx]

        if not building or not items:
            continue

        if items:
            parsed = parse_items(items)
            # print("PARSED:", parsed)

            for name, amount in parsed:
                parsed_rows.append((building, name, amount))

    # -----------------------------
    # Rebuild table
    # -----------------------------
    con.execute("DROP TABLE IF EXISTS items_fragments")

    con.execute(
        """
        CREATE TABLE items_fragments (
            building TEXT,
            item_name TEXT,
            amount DOUBLE
        )
    """
    )

    if parsed_rows:
        con.executemany("INSERT INTO items_fragments VALUES (?, ?, ?)", parsed_rows)

    print(f"items_fragments loaded: {len(parsed_rows)} rows")


# -----------------------------
# Endpoint
# -----------------------------


@app.route("/ingest_linnun", methods=["POST"])
def ingest_linnun():
    payload = request.get_json()
    rows = payload.get("rows", [])

    if not rows:
        return {"status": "error", "message": "No data"}, 400

    headers = rows[0]
    data = normalize_data(rows[1:])

    # -----------------------------
    # CLEAN HEADERS
    # -----------------------------
    headers = [clean_col(h) for h in headers]

    con = get_connection()

    placeholders = ", ".join(["?"] * len(headers))

    # -----------------------------
    # RAW TABLE (all TEXT)
    # -----------------------------
    con.execute("DROP TABLE IF EXISTS raw_linnun")

    raw_cols = ", ".join([f'"{h}" TEXT' for h in headers])
    con.execute(f"CREATE TABLE raw_linnun ({raw_cols})")

    con.executemany(f"INSERT INTO raw_linnun VALUES ({placeholders})", data)

    # -----------------------------
    # TYPED TABLE (strict schema)
    # -----------------------------
    schema = infer_schema(headers, data)

    con.execute("DROP TABLE IF EXISTS typed_linnun")

    typed_cols = ", ".join([f'"{name}" {dtype}' for name, dtype in schema])
    con.execute(f"CREATE TABLE typed_linnun ({typed_cols})")

    con.executemany(f"INSERT INTO typed_linnun VALUES ({placeholders})", data)

    # -----------------------------
    # CURATED LINNUN (explicit, correct)
    # -----------------------------
    con.execute("DROP TABLE IF EXISTS curated_linnun")

    con.execute(
        """
        CREATE TABLE curated_linnun AS
        SELECT
            tl.* EXCLUDE ("rank"),

            tl."rank" AS linnun_rank,

            CASE
                WHEN width = height THEN 'B'
                WHEN width > height THEN 'H'
                ELSE 'V'
            END AS orientation,

            width * height AS area,
            (width * height) + COALESCE(tl.roads, 0) AS footprint

        FROM (
            SELECT
                *,
                TRY_CAST(split_part(size, 'x', 1) AS DOUBLE) AS height,
                TRY_CAST(split_part(size, 'x', 2) AS DOUBLE) AS width
            FROM typed_linnun
        ) tl;
    """
    )

    # -----------------------------
    # OPTIONAL: fragments
    # -----------------------------
    populate_items_fragments(con, headers, data)

    return {"status": "ok", "rows": len(data)}


@app.route("/ingest_weights", methods=["POST"])
def ingest_weights():
    payload = request.get_json()
    rows = payload.get("rows", [])

    if not rows:
        return {"status": "error", "message": "No data"}, 400

    con = get_connection()

    # rows now: (profile, mode, attribute, weight)
    profile = rows[0][0]
    mode = rows[0][1]

    # safety check (worth it)
    modes = {r[1] for r in rows}
    if len(modes) != 1:
        return {"status": "error", "message": "Multiple modes in payload"}, 400

    # -----------------------------
    # DELETE existing profile+mode
    # -----------------------------
    con.execute(
        "DELETE FROM config_weights WHERE profile = ? AND mode = ?",
        [profile, mode],
    )

    # -----------------------------
    # INSERT new weights
    # -----------------------------
    con.executemany(
        """
        INSERT INTO config_weights (profile, mode, attribute, weight)
        VALUES (?, ?, ?, ?)
        """,
        rows,
    )

    return {
        "status": "ok",
        "profile": profile,
        "mode": mode,
        "rows": len(rows),
    }


@app.route("/efficiency", methods=["GET"])
def efficiency():
    profile = request.args.get("profile", "TedMilitary")

    con = get_connection()

    # -----------------------------
    # 1. Get Linnun column order
    # -----------------------------
    attr_rows = con.execute(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_name = 'curated_linnun'
        ORDER BY ordinal_position
    """
    ).fetchall()

    linnun_cols = [r[0] for r in attr_rows]

    # -----------------------------
    # 2. Get non-zero weights
    # -----------------------------
    weight_rows = con.execute(
        """
        SELECT attribute, weight
        FROM config_weights
        WHERE profile = ?
          AND mode = 'attributes'
          AND weight != 0
        """,
        [profile],
    ).fetchall()

    weight_lookup = {k: v for k, v in weight_rows}

    # -----------------------------
    # 3. Final attribute list (ordered)
    # -----------------------------
    attr_cols = [c for c in linnun_cols if c in weight_lookup]

    if not attr_cols:
        return {"status": "error", "message": "No active attributes"}, 400

    # -----------------------------
    # 4. Build SQL fragments
    # -----------------------------
    attr_list_sql = ",\n            ".join(attr_cols)

    attr_select_sql = ",\n        ".join(
        [
            f"""
        COALESCE(
            MAX(CASE
                WHEN a.attribute = '{attr}'
                THEN a.value * a.weight
            END),
        0) AS "{attr}"
        """
            for attr in attr_cols
        ]
    )

    # -----------------------------
    # 5. Main query
    # -----------------------------
    query = f"""
    WITH attr AS (
        SELECT
            building,
            attribute,
            value
        FROM curated_linnun
        UNPIVOT (
            value FOR attribute IN (
                {attr_list_sql}
            )
        )
    ),

    attr_join AS (
        SELECT
            a.building,
            a.attribute,
            a.value,
            w.weight
        FROM attr a
        JOIN config_weights w
          ON w.profile = '{profile}'
         AND w.mode = 'attributes'
         AND w.attribute = a.attribute
    ),

    attr_weight AS (
        SELECT
            building,
            SUM(value * weight) AS attr_weight
        FROM attr_join
        GROUP BY building
    ),

    item_weight AS (
        SELECT
            f.building,
            SUM(f.amount * w.weight) AS item_weight
        FROM items_fragments f
        JOIN config_weights w
          ON w.profile = '{profile}'
         AND w.mode = 'items'
         AND w.attribute = f.item_name
        GROUP BY f.building
    ),

    total AS (
        SELECT
            cl.building,
            COALESCE(a.attr_weight, 0) +
            COALESCE(i.item_weight, 0) AS total_weight
        FROM curated_linnun cl
        LEFT JOIN attr_weight a USING (building)
        LEFT JOIN item_weight i USING (building)
    ),

    scored AS (
        SELECT
            cl.building AS "Building",
            cl.event AS "Event",
            cl.linnun_rank,
            cl.efficiency AS ln_efficiency,
            t.total_weight,
            cl.footprint,

            t.total_weight / NULLIF(cl.footprint, 0) AS efficiency

        FROM curated_linnun cl
        JOIN total t USING (building)
    ),

    ranked AS (
        SELECT *,
            RANK() OVER (ORDER BY efficiency DESC) AS efficiency_rank
        FROM scored
    )

    SELECT
        r."Building",
        r."Event",
        r.linnun_rank,
        r.ln_efficiency,
        r.efficiency,
        r.total_weight,
        r.efficiency_rank,
        (r.linnun_rank + r.efficiency_rank) / 2.0 AS combined_rank,
        {attr_select_sql}

    FROM ranked r
    LEFT JOIN attr_join a
      ON r."Building" = a.building

    GROUP BY
        r."Building",
        r."Event",
        r.linnun_rank,
        r.ln_efficiency,
        r.efficiency,
        r.total_weight,
        r.efficiency_rank

    ORDER BY r.efficiency_rank ASC
    """

    # Debug if needed
    # print(query)

    result = con.execute(query).fetchall()
    columns = [desc[0] for desc in con.description]
    rows = [list(row) for row in result]

    return {
        "status": "ok",
        "columns": columns,
        "rows": rows,
        "weights": weight_lookup,
    }


@app.route("/config_weights", methods=["GET"])
def get_config_weights():
    con = get_connection()

    rows = con.execute(
        """
        SELECT profile, mode, attribute, weight
        FROM config_weights
        ORDER BY profile, attribute
    """
    ).fetchall()

    return {"status": "ok", "rows": rows}


if __name__ == "__main__":
    app.run(port=5000)
