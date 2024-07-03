import asyncio
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import calendar
from loguru import logger
from supabase import create_client, Client
from rich.progress import Progress
from data_fetcher import fetch_klines, upsert_klines
import aiohttp
from dateutil.parser import parse as parse_date

async def get_available_timeframes(supabase: Client, symbol: str):
    response = supabase.table('candles').select('timeframe', count='exact').eq('symbol', symbol).group('timeframe').execute()
    return [item['timeframe'] for item in response.data if item['count'] > 0]

async def fill_data_gaps(supabase: Client, symbol: str, start_date: str, end_date: str, timeframes: list, config, batch_size: int):
    logger.debug(f"Testing and filling data gaps in {symbol} from {start_date} to {end_date}")

    start = parse_date(start_date)
    end = parse_date(end_date)

    logger.debug(f"Processing timeframes: {timeframes}")

    async with aiohttp.ClientSession() as session:
        for timeframe in timeframes:
            logger.debug(f"Processing timeframe: {timeframe}")
            
            # Calculate the timedelta based on the timeframe
            if timeframe == '1W':
                delta = timedelta(weeks=1)
            elif timeframe == '1M':
                delta = relativedelta(months=1)
            elif timeframe.isdigit():
                delta = timedelta(minutes=int(timeframe))
            elif timeframe.endswith('m'):
                delta = timedelta(minutes=int(timeframe[:-1]))
            elif timeframe.endswith('h'):
                delta = timedelta(hours=int(timeframe[:-1]))
            elif timeframe.endswith('d'):
                delta = timedelta(days=int(timeframe[:-1]))
            else:
                logger.warning(f"Unsupported timeframe: {timeframe}")
                continue

            with Progress() as progress:
                if isinstance(delta, relativedelta):
                    # For monthly timeframes, estimate the total number of months
                    total_months = (end.year - start.year) * 12 + end.month - start.month
                    task = progress.add_task(f"[green]Checking and filling gaps in {timeframe}...", total=total_months)
                else:
                    task = progress.add_task(f"[green]Checking and filling gaps in {timeframe}...", total=(end - start).total_seconds() / delta.total_seconds())

                current_date = start
                while current_date < end:
                    batch_end = min(current_date + delta * batch_size, end)
                    
                    # Fetch and upsert data for the current batch
                    klines = await fetch_klines(session, symbol, timeframe, current_date, batch_end, config)
                    if klines:
                        await upsert_klines(supabase, klines, symbol, timeframe)
                        logger.debug(f"Filled {len(klines)} records for {timeframe} from {current_date} to {batch_end}")
                    else:
                        logger.warning(f"No data available to fill gap from {current_date} to {batch_end} for {timeframe}")
                    
                    current_date = batch_end
                    
                    if isinstance(delta, relativedelta):
                        progress.update(task, advance=1)
                    else:
                        progress.update(task, advance=batch_size)

            logger.debug(f"Completed processing for timeframe {timeframe}")

async def run_gap_test_and_fill(symbol: str, start_date: str, end_date: str, config, timeframes: list, batch_size: int, supabase: Client):
    logger.debug(f"Testing and filling data gaps in {symbol} from {start_date} to {end_date}")
    logger.debug(f"Processing timeframes: {timeframes}")
    
    await fill_data_gaps(supabase, symbol, start_date, end_date, timeframes, config, batch_size)

