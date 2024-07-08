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
from websockets import exceptions as websockets_exceptions
from data_fetcher import fetch_klines, upsert_klines, upsert_klines_websocket
from test_data_gaps import get_available_timeframes
from datetime import datetime, timedelta
from dateutil.parser import parse as parse_date
from dashboard import create_dashboard
from indicators import calculate_rsi, calculate_macd, calculate_bollinger_bands, calculate_sma, calculate_fibonacci_retracement  # Import the Fibonacci function
import numpy as np  # Import numpy for NaN handling

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
        if 'data' in data and len(data['data']) > 0:
            kline = data['data'][0]
            kline_data = {
                'start': datetime.fromtimestamp(int(kline['start']) // 1000).isoformat(),
                'open': float(kline['open']),
                'high': float(kline['high']),
                'low': float(kline['low']),
                'close': float(kline['close']),
                'volume': float(kline['volume']),
                'rsi': None  # We'll calculate this later
            }
            
            # Check if the kline data is for a completed candle
            if kline['confirm']:
                await upsert_klines_websocket(pool, [kline_data], symbol, timeframe)
                logger.debug(f"Upserted completed kline data for {symbol} ({timeframe})")
            
            return kline_data
        else:
            logger.warning(f"Received unexpected message format: {message}")
    except Exception as e:
        logger.error(f"Error parsing kline message: {e}")
    return None

async def update_indicators(symbol, timeframe, kline_data, config, session):
    try:
        # Fetch recent data (assuming fetch_klines retrieves historical data)
        end_time = datetime.now()
        delta = timedelta(minutes=int(timeframe) * 50)  # Adjust for desired period
        start_time = end_time - delta
        recent_klines = await fetch_klines(session, symbol, timeframe, start_time, end_time, config)

        # Process fetched data
        if recent_klines and isinstance(recent_klines, list):
            # Extract closing prices from recent klines
            closes = [float(kline[4]) for kline in recent_klines]
            highs = [float(kline[2]) for kline in recent_klines]
            lows = [float(kline[3]) for kline in recent_klines]

            # Add the most recent close price from kline_data (optional)
            closes.append(kline_data['close'])
            highs.append(kline_data['high'])
            lows.append(kline_data['low'])

            # Debug: Print closing prices
            #print(f"Closing prices for {symbol} {timeframe}: {closes}")

            # Calculate RSI using your function
            rsi_values = calculate_rsi(closes)
            kline_data['rsi'] = rsi_values[-1] if len(rsi_values) > 0 else np.nan

            # Calculate MACD using your function
            macd_line, signal_line, macd_histogram = calculate_macd(closes)

            # Debug: Print MACD values
            #print(f"MACD for {symbol} {timeframe}: macd_line={macd_line}, signal_line={signal_line}, histogram={macd_histogram}")

            kline_data['macd'] = {
                'macd_line': macd_line[-1] if len(macd_line) > 0 else np.nan,
                'signal_line': signal_line[-1] if len(signal_line) > 0 else np.nan,
                'histogram': macd_histogram[-1] if len(macd_histogram) > 0 else np.nan
            }

            # Calculate Bollinger Bands using your function
            middle_band, upper_band, lower_band = calculate_bollinger_bands(closes)

            # Debug: Print Bollinger Bands values
            #print(f"Bollinger Bands for {symbol} {timeframe}: middle_band={middle_band}, upper_band={upper_band}, lower_band={lower_band}")

            kline_data['bollinger_bands'] = {
                'middle_band': middle_band[-1] if len(middle_band) > 0 else np.nan,
                'upper_band': upper_band[-1] if len(upper_band) > 0 else np.nan,
                'lower_band': lower_band[-1] if len(lower_band) > 0 else np.nan
            }

            # Calculate SMA using your function
            sma_values = calculate_sma(closes)

            # Debug: Print SMA values
            #print(f"SMA for {symbol} {timeframe}: {sma_values}")

            kline_data['sma'] = sma_values[-1] if len(sma_values) > 0 else np.nan

            # Calculate Fibonacci Retracement using your function
            high = max(highs)
            low = min(lows)
            fibonacci_levels = calculate_fibonacci_retracement(high, low)

            # Debug: Print Fibonacci levels
            #print(f"Fibonacci levels for {symbol} {timeframe}: {fibonacci_levels}")

            kline_data['fibonacci'] = fibonacci_levels

        else:
            logger.warning(f"No valid recent klines data received for {symbol} {timeframe}")
            kline_data['rsi'] = np.nan
            kline_data['macd'] = {
                'macd_line': np.nan,
                'signal_line': np.nan,
                'histogram': np.nan
            }
            kline_data['bollinger_bands'] = {
                'middle_band': np.nan,
                'upper_band': np.nan,
                'lower_band': np.nan
            }
            kline_data['sma'] = np.nan
            kline_data['fibonacci'] = {
                '0.0%': np.nan,
                '23.6%': np.nan,
                '38.2%': np.nan,
                '50.0%': np.nan,
                '61.8%': np.nan,
                '100.0%': np.nan
            }

    except Exception as e:
        logger.error(f"Error calculating indicators for {symbol} {timeframe}: {e}")
        kline_data['rsi'] = np.nan
        kline_data['macd'] = {
            'macd_line': np.nan,
            'signal_line': np.nan,
            'histogram': np.nan
        }
        kline_data['bollinger_bands'] = {
            'middle_band': np.nan,
            'upper_band': np.nan,
            'lower_band': np.nan
        }
        kline_data['sma'] = np.nan
        kline_data['fibonacci'] = {
            '0.0%': np.nan,
            '23.6%': np.nan,
            '38.2%': np.nan,
            '50.0%': np.nan,
            '61.8%': np.nan,
            '100.0%': np.nan
        }

    return kline_data

async def fetch_missing_data(session, pool, symbol, timeframe, last_timestamp, config):
    end_time = datetime.now()
    start_time = datetime.fromtimestamp(last_timestamp)
    
    if (end_time - start_time).total_seconds() > 60:  # If gap is more than 1 minute
        logger.debug(f"Fetching missing data from {start_time} to {end_time}")
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
            await upsert_klines(pool, formatted_klines, symbol, timeframe)
            logger.debug(f"Upserted {len(formatted_klines)} missing klines")
        else:
            logger.warning("No valid klines data received")

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
            elif timeframe == 'W':
                delta = timedelta(weeks=1)
            elif timeframe == 'M':
                delta = timedelta(days=30)  # Approximation
            elif timeframe.endswith('m'):
                delta = timedelta(minutes=int(timeframe[:-1]))
            elif timeframe.endswith('h'):
                delta = timedelta(hours=int(timeframe[:-1]))
            elif timeframe == 'D':
                delta = timedelta(days=1)  # Fix for daily timeframe
            else:
                logger.warning(f"Unsupported timeframe: {timeframe}")
                continue

async def check_data_health(pool, symbol, timeframe, start_date, end_date, config):
    try:
        try:
            start_date = parse_date(start_date) if isinstance(start_date, str) else start_date
            end_date = parse_date(end_date) if isinstance(end_date, str) else end_date
        except ValueError:
            logger.warning(f"Invalid date format for start_date or end_date. Using defaults.")
            # Set default values or handle the error differently
        await fill_data_gaps(pool, symbol, start_date.isoformat(), end_date.isoformat(), [timeframe], config)
        return True
    except Exception as e:
        logger.error(f"Error checking data health for {symbol} ({timeframe}): {e}")
        logger.debug(f"Error details: {type(e).__name__}: {str(e)}")
        logger.debug(f"Error traceback: {traceback.format_exc()}")
        return False

async def start_websocket_connections(symbols: list, timeframes: list, start_date: str, config):
    pool = create_client(config.SUPABASE_URL, config.SUPABASE_SERVICE_KEY)
    
    async with aiohttp.ClientSession() as session:
        websockets = {}
        symbols_data = {symbol: {tf: {'kline': None, 'is_healthy': False} for tf in timeframes} for symbol in symbols}
        
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
                        logger.debug(f"Received message: {message}")
                        
                        data = json.loads(message)
                        if 'topic' in data:
                            current_timeframe = data['topic'].split('.')[1]
                            kline = await handle_kline_message(message, pool, symbol, current_timeframe)
                        
                            if kline:
                                # Calculate and update RSI, MACD, Bollinger Bands, and SMA
                                kline = await update_indicators(symbol, current_timeframe, kline, config, session)
                                
                                # Check data health
                                end_date = datetime.now()
                                is_healthy = await check_data_health(pool, symbol, current_timeframe, parse_date(start_date), end_date, config)
                                
                                # Update only if the timeframe exists in symbols_data[symbol]
                                if current_timeframe in symbols_data[symbol]:
                                    symbols_data[symbol][current_timeframe] = {'kline': kline, 'is_healthy': is_healthy}
                                    layout.update(create_dashboard(symbols_data))
                                    live.update(layout)
                        elif 'success' in data and data['success'] and data['op'] == 'subscribe':
                            logger.info(f"Successfully subscribed: {data}")
                        else:
                            logger.warning(f"Received unexpected message format: {message}")
                
                except asyncio.TimeoutError:
                    logger.warning("WebSocket timeout, reconnecting...")
                    for symbol in symbols:
                        websockets[symbol] = await create_ws_connection(config.BYBIT_WS_URL)
                        for timeframe in timeframes:
                            await subscribe_to_kline(websockets[symbol], symbol, timeframe)
                
                except websockets_exceptions.ConnectionClosed as e:
                    logger.error(f"WebSocket connection closed: {e}")
                    await asyncio.sleep(5)
                    for symbol in symbols:
                        websockets[symbol] = await create_ws_connection(config.BYBIT_WS_URL)
                        for timeframe in timeframes:
                            await subscribe_to_kline(websockets[symbol], symbol, timeframe)
                
                except Exception as e:
                    logger.error(f"Unhandled error in WebSocket connection: {e}")
                    logger.debug(f"Error details: {type(e).__name__}: {str(e)}")
                    logger.debug(f"Error traceback: {traceback.format_exc()}")
                    await asyncio.sleep(5)
                    for symbol in symbols:
                        websockets[symbol] = await create_ws_connection(config.BYBIT_WS_URL)
                        for timeframe in timeframes:
                            await subscribe_to_kline(websockets[symbol], symbol, timeframe)
    
    await asyncio.gather(*[ws.close() for ws in websockets.values()])
    await pool.close()