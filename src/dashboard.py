from rich.table import Table
from rich.panel import Panel
from datetime import datetime
from dateutil.parser import parse as parse_date

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
        rsi = data.get('rsi')  # Assuming 'rsi' key exists in the data

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
          f"{kline['rsi']:.2f}" if kline['rsi'] is not None else 'NaN',
          # Add the RSI value to the row
          # str(rsi) if rsi is not None else "NaN",  # Handle potential missing RSI values
        )
      else:
        table.add_row(str(symbol), str(timeframe), health_status, "", "", "", "", "", "")

  return Panel(table)
