import duckdb
import os
import time

sql_file = "sql/mortgage_etl.sql"
print(f"Starting Mortgage ETL Pipeline via DuckDB...")
print(f"Reading SQL script from {sql_file}")

with open(sql_file, 'r') as f:
    sql_script = f.read()

# Connect to in-memory DuckDB
con = duckdb.connect(database=':memory:')

start_time = time.time()
try:
    # Execute the entire script
    con.execute(sql_script)
    print("✅ ETL Pipeline completed successfully!")
    print(f"⏱️  Execution time: {time.time() - start_time:.2f} seconds.")
    print("📁 Processed CSV files have been exported to the 'processed_data' directory.")
except Exception as e:
    print("❌ Error during ETL execution:")
    print(e)
