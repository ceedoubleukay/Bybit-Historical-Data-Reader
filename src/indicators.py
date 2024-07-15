import pandas as pd
import ta

def calculate_rsi(prices, period=14):
    if len(prices) < period:
        return None  # Not enough data to calculate RSI

    df = pd.DataFrame(prices, columns=['close'])
    rsi = ta.momentum.RSIIndicator(df['close'], window=period).rsi()
    return rsi.tolist()