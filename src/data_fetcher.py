import asyncio
import pandas as pd
import datetime
from loguru import logger
import aiohttp
from supabase import create_client, Client
from rich.progress import Progress
import indicators

from config import Config

async def create_schema(supabase: Client):
    supabase.table('candles').insert({
        'symbol': 'temp',
        'timeframe': 'temp',
        'datetime': datetime.datetime.now().isoformat(),
        'open': 0,
        'high': 0,
        'low': 0,
        'close': 0,
        'volume': 0,
    }, upsert=True)

async def fetch_klines(session, symbol, timeframe, start_time, end_time, config):
    """
    Fetches kline data from Bybit API and calculates RSI. Handles cases with insufficient data.

    Args:
        session (aiohttp.ClientSession): An aiohttp client session.
        symbol (str): The trading symbol (e.g., BTCUSDT).
        interval (str): The candlestick timeframe (e.g., "1", "5m", etc.).
        start_time (datetime.datetime): The start time for fetching data.
        end_time (datetime.datetime): The end time for fetching data.
        config (Config): Configuration object containing API details.

    Returns:
        list: A list of klines with RSI values appended (or None if insufficient data).
    """

    params = {
        "category": "linear",
        "symbol": symbol,
        "interval": timeframe,
        "start": int(start_time.timestamp() * 1000),
        "end": int(end_time.timestamp() * 1000),
        "limit": 1000,  # Adjust limit based on your needs
    }
    url = f"{config.BYBIT_REST_URL}/v5/market/kline"
    logger.debug(f"Fetching klines from API: {url}")
    logger.debug(f"Request parameters: {params}")

    async with session.get(url, params=params) as response:
        logger.debug(f"API response status: {response.status}")
        if response.status == 200:
            data = await response.json()
            logger.debug(f"API response data: {data}")
            result = data.get('result', {}).get('list', [])

            # Check if data is empty or insufficient for RSI calculation
            if not result or len(result) < 14:  # Assuming RSI window is 14
                logger.warning(f"Insufficient data for RSI calculation. Fetched {len(result)} klines (needed at least 14)")
                return None

            # Log closing prices before calculation
            closing_prices = [float(k[4]) for k in result]  # Assuming close price is at index 4
            logger.debug(f"Extracted closing prices: {closing_prices}")

            # Calculate RSI and log the value
            rsi = indicators.calculate_rsi(closing_prices)
            logger.debug(f"Calculated RSI: {rsi}")

            # Append RSI to each kline
            for kline in result:
                kline.append(rsi)  # Assuming kline is a mutable list

            return result
        else:
            error_message = await response.text()
            logger.error(f"Failed to fetch klines. Status code: {response.status}, Error message: {error_message}")
            return []

async def upsert_klines(supabase: Client, klines, symbol, timeframe):
    data = [
        {
            'symbol': symbol,
            'timeframe': timeframe,
            'datetime': datetime.datetime.fromtimestamp(int(k[0])/1000).isoformat(),
            'open': float(k[1]),
            'high': float(k[2]),
            'low': float(k[3]),
            'close': float(k[4]),
            'volume': float(k[5]),
            'rsi': float(k[6]), # Add RSI to the data
        }
        for k in klines
    ]
    # Rest of the function remains the same
    temp_df = pd.DataFrame(data)
    temp_df.drop_duplicates(subset=['symbol', 'timeframe', 'datetime'], inplace=True)
    unique_data = temp_df.to_dict('records')

    try:
        response = supabase.table('candles').insert(unique_data).execute()
        result = response.data
        logger.debug(f"Upserted {len(result)} klines to the database.")
    except Exception as e:
        logger.error(f"Error upserting klines to the database: {e}")

async def upsert_klines_websocket(pool, klines, symbol, timeframe):
    try:
        for kline in klines:
            # Debug: Print kline data being upserted
            print(f"Upserting kline data for {symbol} ({timeframe}): {kline}")

            # Prepare the data for upsert
            data = {
                'symbol': symbol,
                'timeframe': timeframe,
                'start': kline['start'],
                'open': kline['open'],
                'high': kline['high'],
                'low': kline['low'],
                'close': kline['close'],
                'volume': kline['volume'],
                'rsi': kline['rsi'],
                'macd_line': kline['macd']['macd_line'],
                'signal_line': kline['macd']['signal_line'],
                'macd_histogram': kline['macd']['histogram'],
                'middle_band': kline['bollinger_bands']['middle_band'],
                'upper_band': kline['bollinger_bands']['upper_band'],
                'lower_band': kline['bollinger_bands']['lower_band'],
                'sma': kline['sma'],
                'fib_0_0': kline['fibonacci']['0.0%'],
                'fib_23_6': kline['fibonacci']['23.6%'],
                'fib_38_2': kline['fibonacci']['38.2%'],
                'fib_50_0': kline['fibonacci']['50.0%'],
                'fib_61_8': kline['fibonacci']['61.8%'],
                'fib_100_0': kline['fibonacci']['100.0%']
            }

            # Perform the upsert operation
            response = await pool.table('klines').upsert(data).execute()
            if response.status_code == 200:
                logger.debug(f"Successfully upserted kline data for {symbol} ({timeframe})")
            else:
                logger.error(f"Failed to upsert kline data for {symbol} ({timeframe}): {response}")

        logger.debug(f"Upserted {len(klines)} klines into Supabase")
    except Exception as e:
        logger.error(f"Error upserting klines into Supabase: {e}")

# async def fetch_initial_data(symbol, timeframes, start_date, config, batch_size=1440, calculate_rsi_func=indicators.calculate_rsi):
async def fetch_initial_data(symbol, timeframes, start_date, config, batch_size=1440):
    logger.debug(f"Fetching initial data for {symbol} (timeframes: {timeframes}) from {start_date} with batch size {batch_size}")

    supabase = create_client(config.SUPABASE_URL, config.SUPABASE_SERVICE_KEY)

    await create_schema(supabase)

    start_time = datetime.datetime.fromisoformat(start_date)
    end_time = datetime.datetime.now()

    for timeframe in timeframes:
        logger.debug(f"Checking for existing data in the database for timeframe {timeframe}...")
        existing_data_response = supabase.table('candles').select('datetime').eq('symbol', symbol).eq('timeframe', timeframe).order('datetime', desc=True).limit(1).execute()
        existing_data = existing_data_response.data
        if existing_data:
            latest_datetime = datetime.datetime.fromisoformat(existing_data[0]['datetime'])
            if latest_datetime >= end_time:
                logger.debug(f"Data for {symbol} (timeframe: {timeframe}) is already up to date")
                continue
            current_start_time = latest_datetime + datetime.timedelta(minutes=1)  # Start from the next minute after the latest data
            logger.debug(f"Found existing data for timeframe {timeframe}. Resuming from {current_start_time}")
        else:
            current_start_time = start_time
            logger.debug(f"No existing data found for timeframe {timeframe}. Fetching from {current_start_time}")
        
        logger.debug("Fetching latest RSI for", timeframe)
        latest_data_response = supabase.table('candles').select('rsi').eq('symbol', symbol).eq('timeframe', timeframe).order('datetime', desc=True).limit(1).execute()
        latest_data = latest_data_response.data
        latest_rsi = None
        if latest_data:
            latest_rsi = latest_data[0]['rsi']

        async with aiohttp.ClientSession() as session:
            with Progress() as progress:
                task = progress.add_task(f"[green]Fetching data for {timeframe}...", total=(end_time - current_start_time).total_seconds() / 60)

                while current_start_time < end_time:
                    end_chunk_time = min(current_start_time + datetime.timedelta(minutes=batch_size), end_time)
                    klines = await fetch_klines(session, symbol, timeframe, current_start_time, end_chunk_time, config)
                    if klines:
                        logger.debug(f"Fetched {len(klines)} klines for timeframe {timeframe}. Upserting to database...")
                        await upsert_klines(supabase, klines, symbol, timeframe)
                        progress.update(task, advance=(end_chunk_time - current_start_time).total_seconds() / 60)
                    else:
                        logger.warning(f"No klines fetched for the period from {current_start_time} to {end_chunk_time} for timeframe {timeframe}")
                    current_start_time = end_chunk_time  # Move the start time to the end of the current chunk
                    await asyncio.sleep(0.5)  # Rate limiting

    logger.debug("Initial data fetching completed for all timeframes")