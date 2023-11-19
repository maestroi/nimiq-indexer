from fastapi import FastAPI, HTTPException
from fastapi.responses import RedirectResponse
from typing import Optional
import uvicorn
import psycopg2
import os
from time import sleep

# Reuse your database connection logic
def create_db_connection(host, database, user, password, retry_count=5, delay=5):
    attempt = 0
    while attempt < retry_count:
        try:
            conn = psycopg2.connect(
                host=host,
                database=database,
                user=user,
                password=password
            )
            print("Database connection established")
            return conn
        except psycopg2.OperationalError as e:
            attempt += 1
            print(f"Database connection failed. Attempt {attempt}/{retry_count}. Retrying in {delay} seconds...")
            sleep(delay)
    
    print("Failed to connect to the database after several attempts.")
    return None

# Initialize the FastAPI app
app = FastAPI()

# Database connection
DB_HOST = os.getenv('DB_HOST', 'localhost')
DB_NAME = os.getenv('DB_NAME', 'blockchain')
DB_USER = os.getenv('DB_USER', 'username')
DB_PASS = os.getenv('DB_PASS', 'password')
db_conn = create_db_connection(DB_HOST, DB_NAME, DB_USER, DB_PASS)


@app.get("/", include_in_schema=False)
def root():
    return RedirectResponse(url='/docs')

# Utility function to execute a query and fetch results
def fetch_query_results(query, params=None):
    with db_conn.cursor() as cur:
        cur.execute(query, params)
        return cur.fetchall()

@app.get("/blocks/latest")
async def get_latest_blocks(limit: int = 10):
    query = "SELECT * FROM blocks ORDER BY number DESC LIMIT %s;"
    blocks = fetch_query_results(query, (limit,))
    return blocks

@app.get("/transactions/search")
async def search_transactions(address: str, from_date: Optional[str] = None, to_date: Optional[str] = None):
    # This query needs to be adjusted based on how you store and want to filter transactions
    query = "SELECT * FROM transactions WHERE (from_address = %s OR to_address = %s)"
    params = [address, address]

    if from_date:
        query += " AND timestamp >= %s"
        params.append(from_date)
    if to_date:
        query += " AND timestamp <= %s"
        params.append(to_date)

    transactions = fetch_query_results(query, tuple(params))
    return transactions

@app.get("/blocks/details/{block_number}")
async def get_block_details(block_number: int):
    block_query = "SELECT * FROM blocks WHERE number = %s;"
    block = fetch_query_results(block_query, (block_number,))

    if not block:
        raise HTTPException(status_code=404, detail="Block not found")

    transaction_query = "SELECT * FROM transactions WHERE block_number = %s;"
    transactions = fetch_query_results(transaction_query, (block_number,))
    
    return {"block": block, "transactions": transactions}

@app.get("/blocks/{block_number}")
async def get_block(block_number: int):
    cur = db_conn.cursor()
    cur.execute("SELECT * FROM blocks WHERE number = %s", (block_number,))
    block = cur.fetchone()
    cur.close()

    if block is None:
        raise HTTPException(status_code=404, detail="Block not found")
    return block

@app.get("/transactions/{block_number}")
async def get_transactions(block_number: int):
    cur = db_conn.cursor()
    cur.execute("SELECT * FROM transactions WHERE block_number = %s", (block_number,))
    transactions = cur.fetchall()
    cur.close()

    if not transactions:
        raise HTTPException(status_code=404, detail="Transactions not found for this block")
    return transactions

# Run this application with a command like: uvicorn your_script_name:app --reload
if __name__ == "__main__":
    print("Starting API server...")
    uvicorn.run(app, host="0.0.0.0", port=8000)
