# Bybit Historical Data Reader
 Grabs historical data for Bybit and stores it in a CSV

# Running the Script with Optional RSI Filtering
You can now run the script without the RSI parameters to fetch and store data without applying any RSI filter:

- python DataReader.py --timeframe 15m

To run the script with RSI filtering, you can include the RSI parameters:

- python DataReader.py --timeframe 15m --rsi 14 --rsi-threshold 70 --filter over

RSI parameters are OPTIONAL:
Added default values (None) for --rsi, --rsi-threshold, and --filter arguments.
Modified the script to only calculate RSI and filter data if the RSI parameters are provided.