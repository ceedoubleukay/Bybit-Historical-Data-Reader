import asyncio
import json
import websockets
import datetime
import aiohttp
import traceback
from loguru import logger
from rich.live import Live
from rich.console import Console
from rich.layout import Layout
from supabase import create_client
from config import Config
from websockets import exceptions as websockets_exceptions
from data_fetcher import fetch_klines, upsert_klines, upsert_klines_websocket
from test_data_gaps import get_available_timeframes
from datetime import datetime, timedelta
from dateutil.parser import parse as parse_date
from dashboard import create_dashboard
from indicators import calculate_rsi
import pandas as pd
import ta

console = Console()

async def create_ws_connection(url):
    while True:
        try:
            ws = await websockets.connect(url)
            logger.debug(f"Connected to WebSocket: {url}")
            return ws
        except Exception as e:
            logger.error(f"Failed to connect to WebSocket: {e}")
            await asyncio.sleep(5)

async def subscribe_to_kline(ws, symbol, timeframe):
    subscribe_message = {
        "op": "subscribe",
        "args": [f"kline.{timeframe}.{symbol}"]
    }
    await ws.send(json.dumps(subscribe_message))
    logger.debug(f"Sent subscription message: {subscribe_message}")
    logger.debug(f"Subscribed to kline stream for {symbol} ({timeframe})")

async def handle_kline_message(message, pool, symbol, timeframe):
    try:
        data = json.loads(message)
        logger.debug(f"Received message: {data}")
        if 'data' in data and len(data['data']) > 0:
            kline = data['data'][0]
            
            # Ensure the kline data is in the expected dictionary format
            if isinstance(kline, dict):
                start_timestamp = int(kline['start']) // 1000
                kline_data = {
                    'start': start_timestamp,  # Use timestamp in seconds
                    'open': float(kline['open']),
                    'high': float(kline['high']),
                    'low': float(kline['low']),
                    'close': float(kline['close']),
                    'volume': float(kline['volume'])
                }
                
                # Print the kline data for debugging
                #print(f"Kline data: {kline_data}")
                
                # Upsert the kline data regardless of confirmation status
                logger.debug(f"Upserting kline data: {kline_data}")
                await upsert_klines_websocket(pool, [kline_data], symbol, timeframe)
                
                return kline_data
    except Exception as e:
        logger.error(f"Error handling kline message: {e}")
        logger.error(f"Error details: {type(e).__name__}: {str(e)}")
        logger.error(f"Error traceback: {traceback.format_exc()}")
    return None

async def fetch_missing_data(session, pool, symbol, timeframe, last_timestamp, config):
    end_time = datetime.now()
    start_time = datetime.fromtimestamp(last_timestamp)
    
    if (end_time - start_time).total_seconds() > 60:  # If gap is more than 1 minute
        print(f"Fetching missing data from {start_time} to {end_time}")
        klines = await fetch_klines(session, symbol, timeframe, start_time, end_time, config)
        if klines and isinstance(klines, list):
            # Ensure klines is a list of lists
            formatted_klines = [
                [
                    int(k[0]),
                    float(k[1]),
                    float(k[2]),
                    float(k[3]),
                    float(k[4]),
                    float(k[5])
                ]
                for k in klines
            ]
            print(f"Upserting {len(formatted_klines)} missing klines")
            await upsert_klines(pool, formatted_klines, symbol, timeframe)
        else:
            print("No valid klines data received")

async def maintain_connection(ws):
    while True:
        try:
            await ws.ping()
        except:
            break
        await asyncio.sleep(20)

async def fill_data_gaps(supabase, symbol, start_date, end_date, timeframes, config):
    logger.debug(f"Testing and filling data gaps in {symbol} from {start_date} to {end_date}")

    logger.debug(f"Processing timeframes: {timeframes}")

    async with aiohttp.ClientSession() as session:
        for timeframe in timeframes:
            logger.debug(f"Processing timeframe: {timeframe}")
            # Ensure timeframe is an integer for calculations
            if isinstance(timeframe, str) and timeframe.isdigit():
                timeframe = int(timeframe)

            # Calculate the timedelta based on the timeframe
            if isinstance(timeframe, int):  # Directly use integer values
                delta = timedelta(minutes=timeframe)
            elif timeframe == '1W' or timeframe == 'W':
                delta = timedelta(weeks=1)
            elif timeframe == '1M':
                delta = timedelta(days=30)  # Approximation
            elif timeframe == '1D' or timeframe == 'D':
                delta = timedelta(days=1)
            elif timeframe.endswith('m'):
                delta = timedelta(minutes=int(timeframe[:-1]))
            elif timeframe.endswith('h'):
                delta = timedelta(hours(int(timeframe[:-1])))
            elif timeframe.endswith('d'):
                delta = timedelta(days(int(timeframe[:-1])))
            else:
                logger.warning(f"Unsupported timeframe: {timeframe}")
                continue

async def check_data_health(pool, symbol, timeframe, start_date, end_date, config):
    try:
        # Convert start_date and end_date to datetime objects if they're strings
        start_date = parse_date(start_date) if isinstance(start_date, str) else start_date
        end_date = parse_date(end_date) if isinstance(end_date, str) else end_date
        
        await fill_data_gaps(pool, symbol, start_date.isoformat(), end_date.isoformat(), [timeframe], config)
        return True
    except Exception as e:
        logger.error(f"Error checking data health for {symbol} ({timeframe}): {e}")
        logger.debug(f"Error details: {type(e).__name__}: {str(e)}")
        logger.debug(f"Error traceback: {traceback.format_exc()}")
        return False

async def start_websocket_connections(symbols: list, timeframes: list, start_date: str, config: Config):
    pool = create_client(config.SUPABASE_URL, config.SUPABASE_SERVICE_KEY)
    
    async with aiohttp.ClientSession() as session:
        websockets = {}
        symbols_data = {symbol: {tf: {'kline': None, 'is_healthy': False, 'rsi': 'N/A'} for tf in timeframes} for symbol in symbols}
        
        for symbol in symbols:
            ws = await create_ws_connection(config.BYBIT_WS_URL)
            websockets[symbol] = ws
            for timeframe in timeframes:
                await subscribe_to_kline(ws, symbol, timeframe)
        
        layout = Layout()
        layout.update(create_dashboard(symbols_data))
        
        with Live(layout, console=console, refresh_per_second=1) as live:
            while True:
                try:
                    for symbol, ws in websockets.items():
                        message = await asyncio.wait_for(ws.recv(), timeout=30)
                        
                        data = json.loads(message)
                        if 'topic' in data:
                            current_timeframe = data['topic'].split('.')[1]
                            kline = await handle_kline_message(message, pool, symbol, current_timeframe)
                        
                            if kline:
                                # Check data health
                                end_date = datetime.now()
                                is_healthy = await check_data_health(pool, symbol, current_timeframe, parse_date(start_date), end_date, config)
                                
                                # Fetch previous 14 periods of data if not enough data
                                if current_timeframe.isdigit():
                                    period_days = 14 * int(current_timeframe)
                                elif current_timeframe == 'D':
                                    period_days = 14
                                elif current_timeframe == 'W':
                                    period_days = 14 * 7
                                elif current_timeframe == 'M':
                                    period_days = 14 * 30
                                else:
                                    period_days = 14  # Default to 14 days for unknown timeframes

                                klines = await fetch_klines(session, symbol, current_timeframe, end_date - timedelta(days=period_days), end_date, config)
                                if klines and len(klines) >= 14:
                                    close_prices = [float(k[4]) for k in klines]  # Accessing the close price correctly
                                    
                                    # Use the updated `calculate_rsi` function
                                    rsi_values = calculate_rsi(close_prices)
                                    rsi = rsi_values[-1] if rsi_values is not None else 'N/A'
                                else:
                                    rsi = 'N/A'
                                
                                # Update only if the timeframe exists in symbols_data[symbol]
                                if current_timeframe in symbols_data[symbol]:
                                    symbols_data[symbol][current_timeframe] = {'kline': kline, 'is_healthy': is_healthy, 'rsi': rsi}
                                    layout.update(create_dashboard(symbols_data))
                                    live.update(layout)
                        elif 'success' in data and data['success'] and data['op'] == 'subscribe':
                            pass
                        else:
                            pass
                
                except asyncio.TimeoutError:
                    for symbol in symbols:
                        websockets[symbol] = await create_ws_connection(config.BYBIT_WS_URL)
                        for timeframe in timeframes:
                            await subscribe_to_kline(websockets[symbol], symbol, timeframe)
                
                except websockets_exceptions.ConnectionClosed:
                    await asyncio.sleep(5)
                    for symbol in symbols:
                        websockets[symbol] = await create_ws_connection(config.BYBIT_WS_URL)
                        for timeframe in timeframes:
                            await subscribe_to_kline(websockets[symbol], symbol, timeframe)
                
                except Exception:
                    await asyncio.sleep(5)
                    for symbol in symbols:
                        websockets[symbol] = await create_ws_connection(config.BYBIT_WS_URL)
                        for timeframe in timeframes:
                            await subscribe_to_kline(websockets[symbol], symbol, timeframe)
    
    await asyncio.gather(*[ws.close() for ws in websockets.values()])
    await pool.close()