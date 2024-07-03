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

async def fill_data_gaps(supabase: Client, symbol: str, start_date: str, end_date: str, timeframes: list, config):
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
                delta = timedelta(hours(int(timeframe[:-1])))
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

                # Fetch all data for the given range and timeframe
                response = supabase.table('candles').select('datetime').eq('symbol', symbol).eq('timeframe', timeframe).gte('datetime', start.isoformat()).lte('datetime', end.isoformat()).execute()
                
                existing_datetimes = set(datetime.fromisoformat(item['datetime']) for item in response.data)

                current = start
                while current < end:
                    if current not in existing_datetimes:
                        gap_start = current
                        while current < end and current not in existing_datetimes:
                            if isinstance(delta, relativedelta):
                                current += delta
                                # Ensure we don't go past the end of the month
                                current = current.replace(day=min(current.day, calendar.monthrange(current.year, current.month)[1]))
                            else:
                                current += delta
                        
                        logger.debug(f"Filling gap from {gap_start} to {current} for {timeframe}")
                        klines = await fetch_klines(session, symbol, timeframe, gap_start, current, config)
                        
                        if klines:
                            # Filter out existing klines before upserting
                            new_klines = [
                                kline for kline in klines
                                if datetime.fromtimestamp(kline[0] / 1000) not in existing_datetimes
                            ]
                            if new_klines:
                                await upsert_klines(supabase, new_klines, symbol, timeframe)
                                logger.debug(f"Filled {len(new_klines)} new records for {timeframe} from {gap_start} to {current}")
                                # Update existing_datetimes with new data
                                existing_datetimes.update(datetime.fromtimestamp(kline[0] / 1000) for kline in new_klines)
                            else:
                                logger.debug(f"No new data to fill gap from {gap_start} to {current} for {timeframe}")
                        else:
                            logger.warning(f"No data available to fill gap from {gap_start} to {current} for {timeframe}")
                    else:
                        if isinstance(delta, relativedelta):
                            current += delta
                            # Ensure we don't go past the end of the month
                            current = current.replace(day=min(current.day, calendar.monthrange(current.year, current.month)[1]))
                        else:
                            current += delta
                    
                    progress.update(task, advance=1)

            logger.debug(f"Completed processing for timeframe {timeframe}")

async def run_gap_test_and_fill(symbol: str, start_date: str, end_date: str, config, timeframes: list, log_level: str):
    logger.debug(f"Testing and filling data gaps in {symbol} from {start_date} to {end_date}")
    logger.debug(f"Processing timeframes: {timeframes}")
    
    supabase = create_client(config.SUPABASE_URL, config.SUPABASE_SERVICE_KEY)
    await fill_data_gaps(supabase, symbol, start_date, end_date, timeframes, config)
