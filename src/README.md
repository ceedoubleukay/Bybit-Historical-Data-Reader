# Bybit Historical Data Reader

This project is a comprehensive tool for fetching, storing, and analyzing historical cryptocurrency data from Bybit. It supports multiple symbols and timeframes, provides real-time data updates via WebSocket, and includes features for data health checking and gap filling.

## Features

- Fetch historical kline data from Bybit API
- Real-time data updates via WebSocket
- Store data in Supabase database
- Check and fill data gaps
- Data health monitoring
- Rich console output with live updates
- Configurable logging

## Installation

1. Clone the repository:

```bash
git clone https://github.com/yourusername/bybit-historical-data-reader.git
cd bybit-historical-data-reader
```

2. Install the required dependencies:

```bash
pip install -r requirements.txt
```

3. Set up your environment variables in a `.env` file:
```
SUPABASE_URL=your_supabase_url
SUPABASE_SERVICE_KEY=your_supabase_service_key
BYBIT_API_KEY=your_bybit_api_key
BYBIT_API_SECRET=your_bybit_api_secret
```

## Usage

The main script (`main.py`) provides several options for different use cases:

### Fetch Initial Data

To fetch initial historical data and create the schema:

```bash
python src/main.py --symbol BTCUSDT --timeframes 1,5,15,30,60,120,240,D,W --start-date 2023-01-01 --fetch-initial-data --log-level DEBUG
```

This command will fetch data for BTCUSDT with 1-minute, 5-minute, 15-minute, 30-minute, 1-hour, 2-hour, 4-hour, 1-day, and 1-week timeframes starting from January 1, 2023.

### Start WebSocket Connection

To start a WebSocket connection for real-time data updates:

```bash
python src/main.py --symbol BTCUSDT --timeframes 1,5,15,30,60,120,240,D,W --start-date 2023-01-01 --log-level INFO
```

This will establish a WebSocket connection for BTCUSDT with the specified timeframes, starting from January 1, 2023.

### Test and Fill Data Gaps

To test for data gaps and fill them:

```bash
python src/main.py --symbol BTCUSDT --timeframes 1,5,15,30,60,120,240,D,W --start-date 2023-01-01 --end-date 2023-12-31 --test-gaps --log-level DEBUG
```

This command will check for gaps in the data between January 1, 2023, and December 31, 2023, and attempt to fill them for all supported timeframes.

### Additional Options

- `--batch-size`: Set the batch size for data fetching (default: 1440 minutes)
- `--log-level`: Set the logging level (DEBUG, INFO, WARNING, ERROR)

## Project Structure

- `config.py`: Configuration management using Pydantic
- `dashboard.py`: Creates a rich console dashboard for data visualization
- `data_fetcher.py`: Handles fetching historical data from Bybit API
- `data_health_checker.py`: Checks the health of stored data
- `main.py`: Main entry point with argument parsing and execution flow
- `test_data_gaps.py`: Tests for and fills gaps in historical data
- `websocket_handler.py`: Manages WebSocket connections for real-time data

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the MIT License - see the LICENSE file for details.