import pandas as pd
import datetime
import os
import ccxt
from math import ceil
import schedule
import time


symbols = ['BTCUSD', 'ETHUSD']  # List of symbols to fetch
timeframes = ['1h', '4h']  # List of timeframes to fetch
weeks = 100

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

def get_historical_data(symbol, timeframe, weeks):
    now = datetime.datetime.utcnow()
    bybit = ccxt.bybit({
        'apiKey': api,
        'secret': secret,
        'enableRateLimit': True,
    })

    granularity = timeframe_to_sec(timeframe)  # Convert timeframe to seconds
    total_time = weeks * 7 * 24 * 60 * 60
    run_times = ceil(total_time / (granularity * 200))

    dataframe = pd.DataFrame()

    print(f"Fetching data for {symbol} over the past {weeks} weeks with {timeframe} timeframe...")
    for i in range(run_times):
        since = now - datetime.timedelta(seconds=granularity * 200 * (i + 1))
        since_timestamp = int(since.timestamp()) * 1000  # Convert to milliseconds
        print(f"Fetching data from {since} (timestamp: {since_timestamp})")

        data = bybit.fetch_ohlcv(symbol, timeframe, since=since_timestamp, limit=200)
        df = pd.DataFrame(data, columns=['datetime', 'open', 'high', 'low', 'close', 'volume'])
        df['datetime'] = pd.to_datetime(df['datetime'], unit='ms')
        dataframe = pd.concat([df, dataframe])

    dataframe = dataframe.set_index('datetime')
    dataframe = dataframe[["open", "high", "low", "close", "volume"]]

    return dataframe

def fetch_and_save_data():
    # Loop through each timeframe and save the data for all symbols in a single file per timeframe
    for timeframe in timeframes:
        combined_dataframe = pd.DataFrame()

        for symbol in symbols:
            data = get_historical_data(symbol, timeframe, weeks)
            data['symbol'] = symbol  # Add a column for the symbol
            combined_dataframe = pd.concat([combined_dataframe, data])

        # Determine the file path
        combined_file = os.path.join(script_dir, f'combined-data-{timeframe}.csv')

        if os.path.exists(combined_file):
            print(f"Loading existing data from {combined_file}")
            existing_data = pd.read_csv(combined_file, index_col='datetime', parse_dates=True)
            combined_dataframe = pd.concat([existing_data, combined_dataframe])

        # Remove duplicates by keeping the most recent entries
        combined_dataframe = combined_dataframe[~combined_dataframe.index.duplicated(keep='last')]

        # Save the combined data to CSV
        combined_dataframe.to_csv(combined_file)
        print(f"Combined data for {timeframe} saved to: {combined_file}")

# Schedule task to run every 5 minutes
schedule.every(5).seconds.do(fetch_and_save_data)

# Run indefinitely
while True:
    schedule.run_pending()
    time.sleep(1)
