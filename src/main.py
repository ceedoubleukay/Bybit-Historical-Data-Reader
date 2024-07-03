import argparse
import asyncio
import signal
from loguru import logger
from rich.console import Console
from supabase import create_client
from datetime import datetime
from websocket_handler import start_websocket_connections
from config import load_config
from data_fetcher import fetch_initial_data
from test_data_gaps import run_gap_test_and_fill

console = Console()

def setup_logger(log_level):
    logger.remove()  # Remove default handler
    logger.add(
        "app.log",
        rotation="500 MB",
        level=log_level.upper(),
        format="{time} {level} {message}",
        backtrace=True,
        diagnose=True,
    )
    logger.add(
        lambda msg: print(msg, end=""),
        level=log_level.upper(),
        format="{message}",
        colorize=True,
        backtrace=True,
        diagnose=True,
    )

async def main():
    parser = argparse.ArgumentParser(description='Bybit Historical Data Reader')
    parser.add_argument('--symbol', type=str, help='Trading symbol (e.g., BTCUSDT)', default='BTCUSDT')
    parser.add_argument('--timeframes', type=str, help='Comma-separated timeframes (e.g., 1,5,15)', default='1')
    parser.add_argument('--start-date', type=str, help='Start date (e.g., 2024-07-01)', default='2024-07-01')
    parser.add_argument('--fetch-initial-data', action='store_true', help='Fetch initial data and create schema')
    parser.add_argument('--log-level', type=str, help='Log level (e.g., DEBUG, INFO, WARNING, ERROR)', default='INFO')
    parser.add_argument("--batch-size", type=int, default=1440, help="Batch size in minutes (default: 1440)")
    parser.add_argument("--test-gaps", action='store_true', help="Test for data gaps in Supabase")
    parser.add_argument("--end-date", type=str, help="End date for gap testing (default: current date)", default=None)
    args = parser.parse_args()

    setup_logger(args.log_level)
    
    config = load_config()
    supabase = create_client(config.SUPABASE_URL, config.SUPABASE_SERVICE_KEY)

    # Split the timeframes string into a list
    timeframes = [tf.strip() for tf in args.timeframes.split(',')]

    if args.test_gaps:
        end_date = args.end_date or datetime.now().isoformat()
        await run_gap_test_and_fill(args.symbol, args.start_date, end_date, config, timeframes, args.log_level)
    elif args.fetch_initial_data:
        for timeframe in timeframes:
            existing_data_response = supabase.table('candles').select('datetime').eq('symbol', args.symbol).eq('timeframe', timeframe).order('datetime', desc=True).limit(1).execute()
            existing_data = existing_data_response.data
            if existing_data:
                latest_datetime = existing_data[0]['datetime']
                await fetch_initial_data(args.symbol, timeframe, latest_datetime, config, args.batch_size)
            else:
                await fetch_initial_data(args.symbol, timeframe, args.start_date, config, args.batch_size)
    else:
        try:
            await start_websocket_connections([args.symbol], timeframes, args.start_date, config)
        except KeyboardInterrupt:
            logger.info("Received keyboard interrupt, shutting down...")
        finally:
            # Cancel all running tasks
            tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
            [task.cancel() for task in tasks]
            await asyncio.gather(*tasks, return_exceptions=True)
            logger.info("All tasks have been cancelled")

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    main_task = asyncio.ensure_future(main())
    
    # Add signal handlers
    for signame in ('SIGINT', 'SIGTERM'):
        loop.add_signal_handler(getattr(signal, signame),
                                lambda: asyncio.create_task(main_task.cancel()))
    
    try:
        loop.run_until_complete(main_task)
    except asyncio.CancelledError:
        pass
    finally:
        loop.close()
        logger.info("Event loop closed")
