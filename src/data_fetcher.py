import asyncio
import pandas as pd
import datetime
from loguru import logger
import aiohttp
from supabase import create_client, Client
from rich.progress import Progress

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

async def fetch_klines(session, symbol, interval, start_time, end_time, config: Config):
    params = {
        "category": "linear",
        "symbol": symbol,
        "interval": interval,
        "start": int(start_time.timestamp() * 1000),
        "end": int(end_time.timestamp() * 1000),
        "limit": 1000
    }
    url = f"{config.BYBIT_REST_URL}/v5/market/kline"
    logger.debug(f"Fetching klines from API: {url}")
    logger.debug(f"Request parameters: {params}")

    async with session.get(url, params=params) as response:
        logger.debug(f"API response status: {response.status}")
        if response.status == 200:
            data = await response.json()
            logger.debug(f"API response data: {data}")
            return data.get('result', {}).get('list', [])
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

async def upsert_klines_websocket(supabase: Client, klines, symbol, timeframe):
    data = [
        {
            'symbol': symbol,
            'timeframe': timeframe,
            'datetime': datetime.datetime.fromtimestamp(k['start']).isoformat(),  # Convert timestamp to ISO format
            'open': float(k['open']),
            'high': float(k['high']),
            'low': float(k['low']),
            'close': float(k['close']),
            'volume': float(k['volume']),
        }
        for k in klines
    ]
    
    if not data:
        logger.info(f"No websocket klines to upsert for {symbol} ({timeframe})")
        return

    temp_df = pd.DataFrame(data)
    temp_df.drop_duplicates(subset=['symbol', 'timeframe', 'datetime'], inplace=True)
    unique_data = temp_df.to_dict('records')

    try:
        response = supabase.table('candles').upsert(unique_data).execute()
        result = response.data
        logger.debug(f"Upserted {len(result)} websocket klines to the database.")
    except Exception as e:
        logger.error(f"Error upserting websocket klines to the database: {e}")
        logger.error(f"Error details: {type(e).__name__}: {str(e)}")
        logger.error(f"Error traceback: {traceback.format_exc()}")

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
