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
                
                table.add_row(
                    str(symbol),
                    str(timeframe),
                    health_status,
                    str(timestamp),
                    str(kline['open']),
                    str(kline['high']),
                    str(kline['low']),
                    str(kline['close']),
                    str(kline['volume'])
                )
            else:
                table.add_row(str(symbol), str(timeframe), health_status, "", "", "", "", "", "")
    
    return Panel(table)