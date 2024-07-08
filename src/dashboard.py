from rich.table import Table
from rich.panel import Panel
from datetime import datetime
from dateutil.parser import parse as parse_date
import numpy as np  # Import numpy for NaN handling

def create_dashboard(symbols_data):
    table = Table(title="Cryptocurrency Data")
    table.add_column("Symbol", style="cyan")
    table.add_column("Timeframe", style="cyan")
    table.add_column("Data Health", style="green")
    table.add_column("Timestamp", style="cyan")
    table.add_column("Open", style="magenta")
    table.add_column("High", style="green")
    table.add_column("Low", style="red")
    table.add_column("Close", style="yellow")
    table.add_column("Volume", style="blue")
    table.add_column("RSI", style="cyan")
    table.add_column("MACD Line", style="cyan")
    table.add_column("Signal Line", style="cyan")
    table.add_column("MACD Histogram", style="cyan")
    table.add_column("Middle Band", style="cyan")
    table.add_column("Upper Band", style="cyan")
    table.add_column("Lower Band", style="cyan")
    table.add_column("SMA", style="cyan")
    table.add_column("Fib 0.0%", style="cyan")
    table.add_column("Fib 23.6%", style="cyan")
    table.add_column("Fib 38.2%", style="cyan")
    table.add_column("Fib 50.0%", style="cyan")
    table.add_column("Fib 61.8%", style="cyan")
    table.add_column("Fib 100.0%", style="cyan")

    for symbol, timeframes in symbols_data.items():
        for timeframe, data in timeframes.items():
            kline = data['kline']
            is_healthy = data['is_healthy']
            health_status = "🟢" if is_healthy else "🔴"

            if kline:
                # Handle both Unix timestamp and ISO format
                if isinstance(kline['start'], (int, float)):
                    timestamp = datetime.fromtimestamp(int(kline['start']) // 1000)
                else:
                    timestamp = parse_date(kline['start'])

                # Access the RSI value from the data dictionary
                rsi = kline.get('rsi', np.nan)  # Corrected to access from kline

                # Access the MACD values from the data dictionary
                macd = kline.get('macd', {})  # Corrected to access from kline
                macd_line = macd.get('macd_line', np.nan)
                signal_line = macd.get('signal_line', np.nan)
                macd_histogram = macd.get('histogram', np.nan)

                # Access the Bollinger Bands values from the data dictionary
                bollinger_bands = kline.get('bollinger_bands', {})
                middle_band = bollinger_bands.get('middle_band', np.nan)
                upper_band = bollinger_bands.get('upper_band', np.nan)
                lower_band = bollinger_bands.get('lower_band', np.nan)

                # Access the SMA value from the data dictionary
                sma = kline.get('sma', np.nan)

                # Access the Fibonacci levels from the data dictionary
                fibonacci = kline.get('fibonacci', {})
                fib_0_0 = fibonacci.get('0.0%', np.nan)
                fib_23_6 = fibonacci.get('23.6%', np.nan)
                fib_38_2 = fibonacci.get('38.2%', np.nan)
                fib_50_0 = fibonacci.get('50.0%', np.nan)
                fib_61_8 = fibonacci.get('61.8%', np.nan)
                fib_100_0 = fibonacci.get('100.0%', np.nan)

                table.add_row(
                    str(symbol),
                    str(timeframe),
                    health_status,
                    str(timestamp),
                    str(kline['open']),
                    str(kline['high']),
                    str(kline['low']),
                    str(kline['close']),
                    str(kline['volume']),
                    f"{rsi:.2f}" if not np.isnan(rsi) else 'NaN',
                    f"{macd_line:.2f}" if not np.isnan(macd_line) else 'NaN',
                    f"{signal_line:.2f}" if not np.isnan(signal_line) else 'NaN',
                    f"{macd_histogram:.2f}" if not np.isnan(macd_histogram) else 'NaN',
                    f"{middle_band:.2f}" if not np.isnan(middle_band) else 'NaN',
                    f"{upper_band:.2f}" if not np.isnan(upper_band) else 'NaN',
                    f"{lower_band:.2f}" if not np.isnan(lower_band) else 'NaN',
                    f"{sma:.2f}" if not np.isnan(sma) else 'NaN',
                    f"{fib_0_0:.2f}" if not np.isnan(fib_0_0) else 'NaN',
                    f"{fib_23_6:.2f}" if not np.isnan(fib_23_6) else 'NaN',
                    f"{fib_38_2:.2f}" if not np.isnan(fib_38_2) else 'NaN',
                    f"{fib_50_0:.2f}" if not np.isnan(fib_50_0) else 'NaN',
                    f"{fib_61_8:.2f}" if not np.isnan(fib_61_8) else 'NaN',
                    f"{fib_100_0:.2f}" if not np.isnan(fib_100_0) else 'NaN'
                )
            else:
                table.add_row(str(symbol), str(timeframe), health_status, "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "")

    return Panel(table)