import datetime
from loguru import logger

async def check_data_health(pool, symbol, timeframe, config):
    try:
        # Get the latest candle from the database
        result = await pool.table('candles').select('*').eq('symbol', symbol).eq('timeframe', timeframe).order('datetime', desc=True).limit(1).execute()
        
        if not result.data:
            logger.warning(f"No data found for {symbol} ({timeframe})")
            return False
        
        latest_candle = result.data[0]
        latest_datetime = datetime.datetime.fromisoformat(latest_candle['datetime'])
        current_time = datetime.datetime.now(datetime.timezone.utc)
        
        # Calculate the expected time difference based on the timeframe
        if timeframe.endswith('m'):
            expected_diff = datetime.timedelta(minutes=int(timeframe[:-1]))
        elif timeframe.endswith('h'):
            expected_diff = datetime.timedelta(hours=int(timeframe[:-1]))
        elif timeframe == '1d':
            expected_diff = datetime.timedelta(days=1)
        else:
            logger.error(f"Unsupported timeframe format: {timeframe}")
            return False
        
        actual_diff = current_time - latest_datetime
        
        # Allow for a small buffer (e.g., 2 minutes) to account for processing delays
        buffer = datetime.timedelta(minutes=2)
        
        if actual_diff <= expected_diff + buffer:
            return True
        else:
            logger.warning(f"Data health check failed for {symbol} ({timeframe}). Latest data: {latest_datetime}, Current time: {current_time}")
            return False
    
    except Exception as e:
        logger.error(f"Error checking data health for {symbol} ({timeframe}): {e}")
        return False