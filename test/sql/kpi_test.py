import duckdb

# 1. Connect to the database file
conn = duckdb.connect("analytics.db")

# 2. Read and execute the generated SQL
try:
    with open("generated/sql/compute_daily_sales.sql") as f:
        sql = f.read()
    
    print("⏳ Executing Daily Sales KPI transformation...")
    conn.execute(sql)
    print("✅ Table 'fct_daily_sales' created successfully.\n")

    # 3. Fetch and Print results
    # Using .df() for a pretty table format in the terminal
    results = conn.execute("SELECT * FROM fct_daily_sales ORDER BY sales_date DESC LIMIT 10").df()
    
    print("📊 TOP 10 RECENT DAILY SALES KPIs:")
    print("-" * 60)
    print(results)
    print("-" * 60)

except Exception as e:
    print(f"❌ Error during execution: {e}")

finally:
    conn.close()