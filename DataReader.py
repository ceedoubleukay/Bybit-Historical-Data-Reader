import pandas as pd
import datetime
import os
import ccxt
from math import ceil
import schedule
import time
from dotenv import load_dotenv
from ccxt.base.errors import RequestTimeout
import psycopg2
from psycopg2.extras import execute_values

# Load .env variables
load_dotenv()

symbols = ['BTC/USDT:USDT']  # List of symbols to fetch
timeframes = ['1m']  # List of all possible timeframes to fetch
start_year = 2020

# PostgreSQL connection parameters
db_params = {
    'dbname': os.getenv('DB_NAME'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'host': os.getenv('DB_HOST'),
    'port': os.getenv('DB_PORT')
}

# Get the directory of the current script
script_dir = os.path.dirname(os.path.abspath(__file__))

# Function to convert timeframe to seconds
def timeframe_to_sec(timeframe):
    if 'm' in timeframe:
        return int(''.join([char for char in timeframe if char.isnumeric()])) * 60
    elif 'h' in timeframe:
        return int(''.join([char for char in timeframe if char.isnumeric()])) * 60 * 60
    elif 'd' in timeframe:
        return int(''.join([char for char in timeframe if char.isnumeric()])) * 24 * 60 * 60
    elif 'w' in timeframe:
        return int(''.join([char for char in timeframe if char is_numeric()])) * 7 * 24 * 60 * 60
    elif 'M' in timeframe:
        return int(''.join([char for char in timeframe if char.is_numeric()])) * 30 * 24 * 60 * 60

def get_historical_data(symbol, timeframe, start_time, end_time):
    bybit = ccxt.bybit({
        'apiKey': os.getenv('API_KEY'),
        'secret': os.getenv('API_SECRET'),
        'enableRateLimit': True,
        'timeout': 30000,  # Set the timeout to 30 seconds (30000 milliseconds)
        'options': {
            'defaultType': 'future',  # Specify the market type as 'future'
        },
    })

    granularity = timeframe_to_sec(timeframe)  # Convert timeframe to seconds

    dataframe = pd.DataFrame()

    print(f"Fetching data for {symbol} from {start_time} to {end_time} with {timeframe} timeframe...")
    since = start_time
    while since < end_time:
        since_timestamp = int(since.timestamp()) * 1000  # Convert to milliseconds
        print(f"Fetching data from {since} (timestamp: {since_timestamp})")

        try:
            data = bybit.fetch_ohlcv(symbol, timeframe, since=since_timestamp, limit=200)
        except RequestTimeout as e:
            print(f"Request timeout occurred: {e}")
            # Retry the request or handle the error appropriately
            continue  # Skip to the next iteration of the loop

        df = pd.DataFrame(data, columns=['datetime', 'open', 'high', 'low', 'close', 'volume'])
        df['datetime'] = pd.to_datetime(df['datetime'], unit='ms')
        dataframe = pd.concat([dataframe, df])

        since += datetime.timedelta(seconds=granularity * 200)

    dataframe = dataframe.set_index('datetime')
    dataframe = dataframe[["open", "high", "low", "close", "volume"]]

    return dataframe

def fetch_and_save_data():
    conn = psycopg2.connect(**db_params)
    cur = conn.cursor()

    end_time = datetime.datetime.utcnow()
    start_time = datetime.datetime(start_year, 1, 1)  # Start from January 1st of the specified year

    # Loop through each timeframe and save the data for all symbols in a single file per timeframe
    for timeframe in timeframes:
        for symbol in symbols:
            print(f"Processing {symbol} for timeframe {timeframe}")

            # Check for the last record in the database
            cur.execute("""
                SELECT MAX(datetime) FROM candles 
                WHERE symbol = %s AND timeframe = %s
            """, (symbol, timeframe))
            last_record = cur.fetchone()
            if last_record[0]:
                start_time = last_record[0] + datetime.timedelta(seconds=timeframe_to_sec(timeframe))
                print(f"Resuming from last timestamp: {start_time}")

            data = get_historical_data(symbol, timeframe, start_time, end_time)
            data['symbol'] = symbol  # Add a column for the symbol

            # Prepare data for insertion
            records = [
                (row.symbol, timeframe, row.name, row['open'], row['high'], row['low'], row['close'], row['volume'])
                for row in data.itertuples()
            ]

            if records:
                # Insert new records
                execute_values(cur, """
                    INSERT INTO candles (symbol, timeframe, datetime, open, high, low, close, volume) 
                    VALUES %s 
                    ON CONFLICT (symbol, timeframe, datetime) DO NOTHING
                """, records)
                print(f"Inserted {len(records)} records for {symbol} with {timeframe} timeframe.")

    conn.commit()
    cur.close()
    conn.close()

# Schedule task to run every minute
schedule.every(1).minutes.do(fetch_and_save_data)

# Run indefinitely
while True:
    schedule.run_pending()
    time.sleep(1)
