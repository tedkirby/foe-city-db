import duckdb
import sys

DB_PATH = "/home/ted/foe/city-engine/data/city_engine.duckdb"


def resolve_mode(input_mode):
    input_mode = input_mode.lower()

    options = ["attributes", "items"]

    matches = [m for m in options if m.startswith(input_mode)]

    if len(matches) == 1:
        return matches[0]

    if len(matches) == 0:
        raise ValueError(f"Unknown mode: {input_mode}")

    raise ValueError(f"Ambiguous mode: {input_mode} → {matches}")


def get_valid_attributes(con):
    rows = con.execute("DESCRIBE citycore").fetchall()
    return {r[0] for r in rows}


def main():

    if len(sys.argv) < 2:
        print("❌ Usage: python import_profile_clipboard.py <mode>")
        print("   mode: attributes | items")
        sys.exit(1)

    mode_input = sys.argv[1]

    try:
        mode = resolve_mode(mode_input)
    except Exception as e:
        print(f"❌ {e}")
        sys.exit(1)

    print(f"Mode: {mode}")

    con = duckdb.connect(DB_PATH)

    if mode == "attributes":
        valid_attrs = get_valid_attributes(con)

    print("Paste ConfigWeights (Attribute + Profile column), then Ctrl+D:\n")

    text = sys.stdin.read()
    lines = [l.strip().split("\t") for l in text.strip().split("\n") if l.strip()]

    if not lines:
        print("No input")
        sys.exit(1)

    header = lines[0]

    # --- validate header ---
    if len(header) < 2:
        print("❌ Header must have at least 2 columns: Attribute + Profile")
        sys.exit(1)

    if header[0].strip() != "Attribute":
        print("❌ First column must be 'Attribute'")
        sys.exit(1)

    profile = header[1].strip()
    if not profile:
        print("❌ Profile name (column 2) is empty")
        sys.exit(1)

    data = lines[1:]

    validated = []
    errors = []

    for i, row in enumerate(data, start=2):
        if len(row) == 0:
            continue

        attr = row[0].strip()

        # missing value column → skip
        if len(row) < 2:
            continue

        raw = row[1].strip()

        # blank cell → skip
        if not raw:
            continue

        # validate attribute
        if mode == "attributes":
            if attr not in valid_attrs:
                errors.append(f"Row {i}: invalid attribute '{attr}'")
                continue

        # validate number
        try:
            value = float(raw.replace(",", ""))
        except Exception:
            errors.append(f"Row {i}: invalid number '{raw}'")
            continue

        validated.append((attr, value))

    if errors:
        print("\n❌ Errors:")
        for e in errors[:10]:
            print(e)
        print(f"\n{len(errors)} total errors. Aborting.")
        sys.exit(1)

    # replace profile data
    con.execute(
        "DELETE FROM config_weights WHERE profile = ? AND mode = ?", [profile, mode]
    )

    # insert
    con.executemany(
        """
    INSERT INTO config_weights (profile, mode, attribute, weight)
    VALUES (?, ?, ?, ?)
    """,
        [(profile, mode, attr, val) for (attr, val) in validated],
    )

    print(
        f"\n✅ Loaded {len(validated)} rows into config_weights for [{profile}] ({mode})"
    )


if __name__ == "__main__":
    main()
