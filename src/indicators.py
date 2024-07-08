import talib as ta
import numpy as np
from loguru import logger
import pandas as pd

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
  # Print the timeframe before returning RSI (optional)
  # if timeframe:
  #     print(f"RSI for timeframe: {timeframe}")
  # else:
  #   print("No timeframe provided for RSI calculations")

  return ta.RSI(closing_prices_array, timeperiod)


def calculate_macd(prices, slow=26, fast=12, signal=9):
    """
    Calculate MACD (Moving Average Convergence Divergence) using pandas.
    
    :param prices: List of closing prices.
    :param slow: The period for the slow EMA.
    :param fast: The period for the fast EMA.
    :param signal: The period for the signal line.
    :return: A tuple of (macd_line, signal_line, macd_histogram)
    """
    prices_series = pd.Series(prices)
    exp1 = prices_series.ewm(span=fast, adjust=False).mean()
    exp2 = prices_series.ewm(span=slow, adjust=False).mean()
    macd_line = exp1 - exp2
    signal_line = macd_line.ewm(span=signal, adjust=False).mean()
    macd_histogram = macd_line - signal_line
    
    return macd_line.tolist(), signal_line.tolist(), macd_histogram.tolist()

def calculate_bollinger_bands(prices, window=20, num_std_dev=2):
    """
    Calculate Bollinger Bands using pandas.
    
    :param prices: List of closing prices.
    :param window: The period for the moving average.
    :param num_std_dev: The number of standard deviations for the bands.
    :return: A tuple of (middle_band, upper_band, lower_band)
    """
    prices_series = pd.Series(prices)
    middle_band = prices_series.rolling(window=window).mean()
    std_dev = prices_series.rolling(window=window).std()
    upper_band = middle_band + (std_dev * num_std_dev)
    lower_band = middle_band - (std_dev * num_std_dev)
    
    return middle_band.tolist(), upper_band.tolist(), lower_band.tolist()

def calculate_sma(prices, window=20):
    """
    Calculate Simple Moving Average (SMA) using pandas.
    
    :param prices: List of closing prices.
    :param window: The period for the moving average.
    :return: A list of SMA values.
    """
    prices_series = pd.Series(prices)
    sma = prices_series.rolling(window=window).mean()
    return sma.tolist()

def calculate_fibonacci_retracement(high, low):
    """
    Calculate Fibonacci Retracement levels.
    
    :param high: The highest price point.
    :param low: The lowest price point.
    :return: A dictionary of Fibonacci levels.
    """
    diff = high - low
    levels = {
        '0.0%': high,
        '23.6%': high - 0.236 * diff,
        '38.2%': high - 0.382 * diff,
        '50.0%': high - 0.5 * diff,
        '61.8%': high - 0.618 * diff,
        '100.0%': low
    }
    return levels