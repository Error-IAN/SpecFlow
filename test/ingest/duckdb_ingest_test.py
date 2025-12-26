import duckdb

# Connect to the same DB file used in your ingest script
conn = duckdb.connect("analytics.db")

# Execute and actually PRINT the result
result = conn.execute("SELECT COUNT(*) FROM raw_orders").fetchall()

print(f"Total rows in raw_orders: {result[0][0]}")

# Let's see a sample of the data too
print("\nFirst 5 rows:")
print(conn.execute("SELECT * FROM raw_orders LIMIT 5").df()) # .df() makes it look like a pretty table