import talib as ta
import numpy as np
from loguru import logger


def calculate_rsi(closes, timeperiod=14):
  """
  This function calculates the RSI for a given list of closing prices.

  Args:
      closes (list): A list of closing prices.
      timeperiod (int, optional): The RSI time period. Defaults to 14.

  Returns:
      list: A list of RSI values for each closing price.
  """
  # Convert list to NumPy array
  closing_prices_array = np.array(closes)
  logger.info(closing_prices_array)
  return ta.RSI(closing_prices_array, timeperiod)


# Add functions for other indicators here
# For example:
def calculate_macd(closes, slow_period=26, fast_period=12, signal_period=9):
  """
  This function calculates the MACD for a given list of closing prices.
  """
  # ... implementation for MACD calculation ...
  return macd, macd_signal, macd_hist

# etc.
