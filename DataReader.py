import pandas as pd
import datetime
import os
import ccxt
import schedule
import time
from dotenv import load_dotenv
from ccxt.base.errors import RequestTimeout
from supabase import create_client, Client
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Load environment variables
dotenv_path = '.env'
if os.path.exists(dotenv_path):
    load_dotenv(dotenv_path)

# Initialize Supabase client
supabase_url = os.getenv('SUPABASE_URL')
supabase_key = os.getenv('SUPABASE_SERVICE_KEY')
supabase: Client = create_client(supabase_url, supabase_key)

symbols = ['BTC/USDT']  # List of symbols to fetch
timeframes = ['1M']  # List of all possible timeframes to fetch
start_year = 2024

# Function to convert timeframe to seconds
def timeframe_to_sec(timeframe):
    if 'm' in timeframe:
        return int(''.join([char for char in timeframe if char.isnumeric()])) * 60
    elif 'h' in timeframe:
        return int(''.join([char for char in timeframe if char.isnumeric()])) * 60 * 60
    elif 'd' in timeframe:
        return int(''.join([char for char in timeframe if char.isnumeric()])) * 24 * 60 * 60
    elif 'w' in timeframe:
        return int(''.join([char for char in timeframe if char.isnumeric()])) * 7 * 24 * 60 * 60
    elif 'M' in timeframe:
        return int(''.join([char for char in timeframe if char.isnumeric()])) * 30 * 24 * 60 * 60

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

    logging.info(f"Fetching data for {symbol} from {start_time} to {end_time} with {timeframe} timeframe...")
    since = start_time
    while since < end_time:
        since_timestamp = int(since.timestamp()) * 1000  # Convert to milliseconds
        logging.info(f"Fetching data from {since} (timestamp: {since_timestamp})")

        try:
            data = bybit.fetch_ohlcv(symbol, timeframe, since=since_timestamp, limit=200)
        except RequestTimeout as e:
            logging.error(f"Request timeout occurred: {e}")
            # Retry the request or handle the error appropriately
            continue  # Skip to the next iteration of the loop
        except Exception as e:
            logging.error(f"Error fetching data: {e}")
            continue

        df = pd.DataFrame(data, columns=['datetime', 'open', 'high', 'low', 'close', 'volume'])
        df['datetime'] = pd.to_datetime(df['datetime'], unit='ms')
        dataframe = pd.concat([dataframe, df])

        since += datetime.timedelta(seconds=granularity * 200)

    dataframe = dataframe.set_index('datetime')
    dataframe = dataframe[["open", "high", "low", "close", "volume"]]

    return dataframe

def fetch_and_save_data():
    end_time = datetime.datetime.utcnow()
    start_time = datetime.datetime(start_year, 1, 1)  # Start from January 1st of the specified year

    for timeframe in timeframes:
        for symbol in symbols:
            start_time = datetime.datetime(start_year, 1, 1)  # Reset start_time for each symbol/timeframe

            logging.info(f"Fetching data for {symbol} from {start_time} to {end_time} with {timeframe} timeframe...")
            
            try:
                data = get_historical_data(symbol, timeframe, start_time, end_time)
            except Exception as e:
                logging.error(f"Error fetching data for {symbol} with {timeframe} timeframe: {e}")
                continue

            if data is None or data.empty:
                logging.warning(f"No data fetched for {symbol} with {timeframe} timeframe.")
                continue

            # Prepare data for insertion
            records = [
                {
                    'symbol': symbol, 
                    'timeframe': timeframe, 
                    'datetime': row.Index.strftime('%Y-%m-%d %H:%M:%S'),
                    'open': row.open, 
                    'high': row.high, 
                    'low': row.low, 
                    'close': row.close, 
                    'volume': row.volume
                }
                for row in data.itertuples()
            ]

            if records:
                try:
                    # Insert new records into Supabase
                    logging.info(f"Inserting {len(records)} records into Supabase.")
                    response = supabase.table('candles').insert(records).execute()
                    logging.info(f"Inserted {len(records)} records for {symbol} with {timeframe} timeframe.")
                except Exception as e:
                    logging.error(f"Failed to insert records for {symbol} with {timeframe} timeframe: {e}")

# Schedule task to run every minute
schedule.every(1).second.do(fetch_and_save_data)

# Run indefinitely
while True:
    schedule.run_pending()
    time.sleep(1)
