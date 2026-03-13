import json
import glob
import jmespath
import snowflake.connector

#https://mermaid.live/edit#pako:eNpVjctugzAQRX_FmlUrQVReBntRqYE2m0jtIqtCFlZwMEqwkTFKU-Dfa4iitrOZxz33zgAHVXKgcDyry0EwbdAuKySy9ZKnQtedaVi3R677PG64QY2S_Dqi9cNGoU6otq1l9Xjj1zOE0mE7YxwZUcvTdJPSxf8u-YiyfMtao9r9X2V3USN6zesPYeP_K0Jz63rLj4wemXtgGqVMLwg4UOm6BGp0zx1ouG7YvMIwqwUYwRteALVjyfSpgEJO1tMy-alUc7dp1VcCbPa5s1vflszwrGaVZr8IlyXXqeqlARotCUAH-ALqk1Xk-x6JgmDumGAHrkA9e8ceTqIkCkgSeDicHPhenj6t4jggmMSYhHGchKE__QCeEHXL

# ─── CONFIG ───────────────────────────────────────────────
SNOWFLAKE_CONFIG = {
    "user":       "YOUR_USER",
    "password":   "YOUR_PASSWORD",
    "account":    "YOUR_ACCOUNT",   # e.g. xy12345.us-east-1
    "warehouse":  "YOUR_WAREHOUSE",
    "database":   "YOUR_DB",
    "schema":     "YOUR_SCHEMA",
}

TABLE_NAME = "orders_extracted"
JSON_FOLDER = "./json_files/*.json"

# ─── DEFINE WHAT TO EXTRACT ───────────────────────────────
# Format: ("snowflake_column_name", "jmespath.expression")
FIELD_MAP = [
    ("order_id",      "order_id"),
    ("customer_name", "customer.name"),
    ("city",          "customer.address.city"),
    ("skus",          "items[*].sku"),          # array → stored as string
    ("tags",          "items[*].meta.tag"),     # nested inside array
]

# ─── EXTRACT FROM ONE JSON ────────────────────────────────
def extract_record(data: dict) -> dict:
    record = {}
    for col_name, expression in FIELD_MAP:
        value = jmespath.search(expression, data)
        # If result is a list, serialize it (or flatten as needed)
        record[col_name] = json.dumps(value) if isinstance(value, list) else value
    return record

# ─── LOAD ALL JSON FILES ──────────────────────────────────
def load_all_jsons(pattern: str) -> list[dict]:
    records = []
    for filepath in glob.glob(pattern):
        with open(filepath, "r") as f:
            data = json.load(f)
            # Handle both single object and array of objects at root
            if isinstance(data, list):
                for item in data:
                    records.append(extract_record(item))
            else:
                records.append(extract_record(data))
    print(f"✅ Extracted {len(records)} records from JSON files")
    return records

# ─── PUSH TO SNOWFLAKE ────────────────────────────────────
def push_to_snowflake(records: list[dict]):
    if not records:
        print("No records to insert.")
        return

    conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
    cur = conn.cursor()

    # Auto-create table if not exists
    columns_ddl = ", ".join([f"{col} STRING" for col, _ in FIELD_MAP])
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} ({columns_ddl})
    """)

    # Batch insert
    cols = [col for col, _ in FIELD_MAP]
    placeholders = ", ".join(["%s"] * len(cols))
    col_names = ", ".join(cols)

    rows = [tuple(r.get(col) for col in cols) for r in records]
    cur.executemany(
        f"INSERT INTO {TABLE_NAME} ({col_names}) VALUES ({placeholders})",
        rows
    )

    conn.commit()
    cur.close()
    conn.close()
    print(f"✅ Inserted {len(rows)} rows into Snowflake → {TABLE_NAME}")

# ─── MAIN ─────────────────────────────────────────────────
if __name__ == "__main__":
    records = load_all_jsons(JSON_FOLDER)
    push_to_snowflake(records)
