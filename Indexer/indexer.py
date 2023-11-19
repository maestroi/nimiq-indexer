import asyncio
import websockets
import json
import os
import psycopg2
from time import sleep

# Database configuration
DB_HOST = os.getenv('DB_HOST', 'localhost')
DB_NAME = os.getenv('DB_NAME', 'blockchain')
DB_USER = os.getenv('DB_USER', 'username')
DB_PASS = os.getenv('DB_PASS', 'password')

# Function to create a database connection with retry mechanism
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


# Initialize database connection and create tables
def init_db():
    conn = create_db_connection(DB_HOST, DB_NAME, DB_USER, DB_PASS)
    if conn is None:
        # Handle the error, e.g., exit the program or raise an exception
        raise Exception("Failed to establish database connection.")

    cur = conn.cursor()

    # Create tables if they do not exist
    cur.execute("""
        CREATE TABLE IF NOT EXISTS blocks (
            id SERIAL PRIMARY KEY,
            hash VARCHAR(255),
            size INT,
            batch INT,
            epoch INT,
            version INT,
            number BIGINT,
            timestamp BIGINT,
            parent_hash VARCHAR(255),
            seed TEXT,
            extra_data TEXT,
            state_hash VARCHAR(255),
            body_hash VARCHAR(255),
            history_hash VARCHAR(255),
            type VARCHAR(50),
            is_election_block BOOLEAN,
            parent_election_hash VARCHAR(255)
        );
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS transactions (
            id SERIAL PRIMARY KEY,
            block_id INT,
            hash VARCHAR(255),
            block_number BIGINT,
            from_address VARCHAR(255),
            to_address VARCHAR(255),
            value BIGINT,
            fee INT,
            recipient_data TEXT,
            flags INT,
            validity_start_height BIGINT,
            proof TEXT,
            execution_result BOOLEAN,
            FOREIGN KEY (block_id) REFERENCES blocks(id)
        );
    """)
    conn.commit()
    cur.close()
    return conn

async def get_transactions_by_block_number(url, block_number):
    try:
        async with websockets.connect(url) as websocket:
            payload = json.dumps({
                "jsonrpc": "2.0",
                "id": 1,
                "method": "getTransactionsByBlockNumber",
                "params": [block_number]
            })
            await websocket.send(payload)
            response = await websocket.recv()
            return json.loads(response)
    except websockets.exceptions.WebSocketException as e:
        print(f"WebSocket error occurred: {e}")
        return {}

async def format_transaction(tx):
    return (f"Hash: {tx['hash']}, Block: {tx['blockNumber']}, Timestamp: {tx['timestamp']}, "
            f"Confirmations: {tx['confirmations']}, From: {tx['from']}, To: {tx['to']}, "
            f"Value: {tx['value']}, Fee: {tx['fee']}, ExecutionResult: {tx.get('executionResult', 'N/A')}")

# Function to store block data
def store_block_data(conn, block_data):
    try:
        cur = conn.cursor()
        block_insert_query = """
            INSERT INTO blocks (hash, size, batch, epoch, version, number, timestamp, parent_hash, seed, extra_data, state_hash, body_hash, history_hash, type, is_election_block, parent_election_hash)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id;
        """
        cur.execute(block_insert_query, (
            block_data['hash'],
            block_data['size'],
            block_data['batch'],
            block_data['epoch'],
            block_data['version'],
            block_data['number'],
            block_data['timestamp'],
            block_data['parentHash'],
            block_data['seed'],
            block_data['extraData'],
            block_data['stateHash'],
            block_data['bodyHash'],
            block_data['historyHash'],
            block_data['type'],
            block_data.get('isElectionBlock', False),
            block_data.get('parentElectionHash', '')
        ))
        block_id = cur.fetchone()[0]
        conn.commit()
        return block_id
    except Exception as e:
        print("An error occurred while storing block data:", e)
        conn.rollback()
        return None
    finally:
        cur.close()

# Function to insert a transaction into the database
def store_transactions(conn, block_id, transactions):
    try:
        cur = conn.cursor()
        for transaction in transactions:
            transaction_insert_query = """
                INSERT INTO transactions (block_id, hash, block_number, from_address, to_address, value, fee, recipient_data, flags, validity_start_height, proof, execution_result)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            cur.execute(transaction_insert_query, (
                block_id,
                transaction['hash'],
                transaction['blockNumber'],
                transaction['from'],
                transaction['to'],
                transaction['value'],
                transaction['fee'],
                transaction['recipientData'],
                transaction['flags'],
                transaction['validityStartHeight'],
                transaction['proof'],
                transaction['executionResult']
            ))
        conn.commit()
    except Exception as e:
        print("An error occurred while storing transactions:", e)
        conn.rollback()
    finally:
        cur.close()
        
async def main(db_conn):
    uri = os.getenv('WEBSOCKET_URL', 'wss://rpc-testnet.nimiqcloud.com/ws')
    last_block_timestamp = None

    while True:
        try:
            async with websockets.connect(uri) as websocket:
                # Subscribe to new head blocks
                subscribe_payload = json.dumps({
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": "subscribeForHeadBlock",
                    "params": [True]
                })
                await websocket.send(subscribe_payload)

                # Process incoming messages
                while True:
                    message = await websocket.recv()
                    data = json.loads(message)

                    if 'method' in data and data['method'] == 'subscribeForHeadBlock':
                        block_data = data['params']['result']['data']
                        block_number = block_data['number']
                        epoch_number = block_data['epoch']
                        batch_number = block_data['batch']
                        block_type = block_data['type']
                        block_timestamp = block_data['timestamp']
                        
                        block_time_ms = None
                        if last_block_timestamp is not None:
                            block_time_ms = block_timestamp - last_block_timestamp
                        last_block_timestamp = block_timestamp

                        # Fetch transactions for this block
                        transactions_data = await get_transactions_by_block_number(uri, block_number)
                        transactions = transactions_data.get('result', {}).get('data', [])
                        formatted_transactions = [await format_transaction(tx) for tx in transactions]

                        # Check if there are transactions to store
                        block_id = store_block_data(db_conn, block_data)
                        if transactions:
                            if block_id is not None:
                                store_transactions(db_conn, block_id, transactions)
        
                        # Print all relevant information together
                        block_time_info = f", BlockTime: {block_time_ms:.2f} ms" if block_time_ms is not None else ""
                        print(f"Block: {block_number}, Epoch: {epoch_number}, Batch: {batch_number}, Block type: {block_type} {block_time_info}, Transactions: {formatted_transactions}")
        
        except websockets.exceptions.WebSocketException as e:
            print(f"WebSocket error occurred: {e}")
            print("Attempting to reconnect...")
            await asyncio.sleep(1)  # Wait for 5 seconds before trying to reconnect

if __name__ == "__main__":
    print("Starting indexer...")
    db_conn = init_db()
    try:
        asyncio.run(main(db_conn))
    finally:
        db_conn.close()
