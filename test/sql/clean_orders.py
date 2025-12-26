import duckdb

# 1. Connect to the database
conn = duckdb.connect("analytics.db")

# 2. Get the "Before" count
raw_count = conn.execute("SELECT COUNT(*) FROM raw_orders").fetchone()[0]

# 3. Read and execute the generated SQL
with open("generated/sql/clean_orders.sql") as f:
    sql = f.read()

conn.execute(sql)

# 4. Get the "After" count
clean_count = conn.execute("SELECT COUNT(*) FROM stg_orders_cleaned").fetchone()[0]

# --- PRINT THE OUTPUT ---
print("🚀 TRANSFORMATION REPORT")
print("-" * 30)
print(f"Raw Rows Ingested:   {raw_count}")
print(f"Clean Rows Retained: {clean_count}")
print(f"Rows Filtered Out:   {raw_count - clean_count}")
print("-" * 30)

# 5. Show a sample of the cleaned data
print("\n👀 SAMPLE OF CLEANED DATA (First 3 rows):")
# .df() converts the result to a Pandas DataFrame for a pretty table layout
print(conn.execute("SELECT invoice_id, quantity, unit_price, customer_id FROM stg_orders_cleaned LIMIT 3").df())