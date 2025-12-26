import duckdb
import os

DB_PATH = "./analytics.db"

def main():
    # These will be filled by your generator
    file_path = "/home/error-ian/ETL_PROJECT/Prototype/Verse_0.2/data/1/online_retail_II.xlsx" 
    target_table = "raw_orders"

    if not os.path.exists(file_path):
        raise FileNotFoundError(f"File not found: {file_path}")

    conn = duckdb.connect(DB_PATH)

    # 1. Install and load the spatial extension (handles Excel)
    conn.execute("INSTALL spatial;")
    conn.execute("LOAD spatial;")

    # 2. Use st_read to pull from Excel
# ... inside your template's main() ...
    conn.execute(f"""
        CREATE OR REPLACE TABLE {target_table} AS
        SELECT * FROM st_read('/home/error-ian/ETL_PROJECT/Prototype/Verse_0.2/data/1/online_retail_II.xlsx', layer='Year 2009-2010');
    """)

    conn.close()
    print(f"Successfully loaded Excel into DuckDB table: {target_table}")

if __name__ == "__main__":
    main()