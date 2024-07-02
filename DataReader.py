import pandas as pd
import datetime
import os
import ccxt
import argparse
from dotenv import load_dotenv
from ccxt.base.errors import RequestTimeout
from supabase import create_client, Client
import logging
from ta.momentum import RSIIndicator

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

# Parse CLI arguments
parser = argparse.ArgumentParser(description='Fetch and store historical data from Bybit.')
parser.add_argument('--timeframe', type=str, required=True, help='Timeframe for fetching data (e.g., 15m, 1h).')
parser.add_argument('--rsi', type=int, default=None, help='RSI period (optional).')
parser.add_argument('--rsi-threshold', type=int, default=None, help='RSI threshold value (optional).')
parser.add_argument('--filter', type=str, choices=['over', 'under'], default=None, help='Filter data with RSI over or under the threshold (optional).')
args = parser.parse_args()

symbols = ['BTC/USDT']  # List of symbols to fetch
timeframe = args.timeframe  # Timeframe from CLI arguments
rsi_period = args.rsi  # RSI period from CLI arguments
rsi_threshold = args.rsi_threshold  # RSI threshold from CLI arguments
filter_condition = args.filter  # Filter condition ('over' or 'under')
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

def calculate_rsi(dataframe, period):
    rsi = RSIIndicator(dataframe['close'], window=period)
    dataframe['rsi'] = rsi.rsi()
    return dataframe

def filter_by_rsi(dataframe, threshold, condition):
    if condition == 'over':
        return dataframe[dataframe['rsi'] > threshold]
    elif condition == 'under':
        return dataframe[dataframe['rsi'] < threshold]
    return dataframe

def fetch_and_save_data():
    end_time = datetime.datetime.utcnow()
    start_time = datetime.datetime(start_year, 1, 1)  # Start from January 1st of the specified year

    for symbol in symbols:
        logging.info(f"Fetching data for {symbol} from {start_time} to {end_time} with {timeframe} timeframe...")
        
        try:
            data = get_historical_data(symbol, timeframe, start_time, end_time)
        except Exception as e:
            logging.error(f"Error fetching data for {symbol} with {timeframe} timeframe: {e}")
            continue

        if data is None or data.empty:
            logging.warning(f"No data fetched for {symbol} with {timeframe} timeframe.")
            continue

        # Calculate RSI if the period is provided
        if rsi_period:
            data = calculate_rsi(data, rsi_period)

        # Filter data based on RSI if the threshold and condition are provided
        if rsi_threshold and filter_condition:
            data = filter_by_rsi(data, rsi_threshold, filter_condition)

        if data.empty:
            logging.warning(f"No data after applying RSI filter for {symbol} with {timeframe} timeframe.")
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
                'volume': row.volume,
                'rsi': row.rsi if 'rsi' in row else None
            }
            for row in data.itertuples()
        ]

        if records:
            try:
                # Determine the columns to be inserted dynamically
                columns = ['symbol', 'timeframe', 'datetime', 'open', 'high', 'low', 'close', 'volume']
                if rsi_period:
                    columns.append('rsi')

                # Insert new records into Supabase
                logging.info(f"Inserting {len(records)} records into Supabase.")
                response = supabase.table('candles').upsert(records, on_conflict=['symbol', 'timeframe', 'datetime'], returning='minimal').execute()
                logging.info(f"Inserted {len(records)} records for {symbol} with {timeframe} timeframe.")
            except Exception as e:
                logging.error(f"Failed to insert records for {symbol} with {timeframe} timeframe: {e}")

# Fetch and save data based on provided parameters
fetch_and_save_data()
