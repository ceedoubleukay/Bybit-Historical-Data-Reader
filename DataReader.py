import pandas as pd
import datetime
import os
import ccxt
from math import ceil
import schedule
import time
from dotenv import load_dotenv
from ccxt.base.errors import RequestTimeout

# load .env variables
load_dotenv()

symbols = ['BTC/USDT:USDT']  # List of symbols to fetch
timeframes = ['1m']  # List of all possible timeframes to fetch
start_year = 2020

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
    end_time = datetime.datetime.utcnow()
    start_time = datetime.datetime(start_year, 1, 1)  # Start from January 1st of the specified year

    # Loop through each timeframe and save the data for all symbols in a single file per timeframe
    for timeframe in timeframes:
        combined_dataframe = pd.DataFrame()

        for symbol in symbols:
            # Determine the file path
            combined_file = os.path.join(script_dir, f'combined-data-{timeframe}.csv')

            if os.path.exists(combined_file):
                print(f"Loading existing data from {combined_file}")
                existing_data = pd.read_csv(combined_file, index_col='datetime', parse_dates=True)
                last_timestamp = existing_data.index.max()
                start_time = pd.to_datetime(last_timestamp) + pd.Timedelta(seconds=timeframe_to_sec(timeframe))
            else:
                existing_data = pd.DataFrame()

            data = get_historical_data(symbol, timeframe, start_time, end_time)
            data['symbol'] = symbol  # Add a column for the symbol
            combined_dataframe = pd.concat([existing_data, data])

        # Remove duplicates by keeping the most recent entries
        combined_dataframe = combined_dataframe[~combined_dataframe.index.duplicated(keep='last')]

        # Save the combined data to CSV
        combined_dataframe.to_csv(combined_file)
        print(f"Combined data for {timeframe} saved to: {combined_file}")

# Schedule task to run every minute
schedule.every(1).minutes.do(fetch_and_save_data)

# Run indefinitely
while True:
    schedule.run_pending()
    time.sleep(1)
