CREATE DATABASE bybit_data;

\c bybit_data;

CREATE TABLE candles (
    id SERIAL PRIMARY KEY,
    symbol VARCHAR(20) NOT NULL,
    timeframe VARCHAR(5) NOT NULL,
    datetime TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    open NUMERIC NOT NULL,
    high NUMERIC NOT NULL,
    low NUMERIC NOT NULL,
    close NUMERIC NOT NULL,
    volume NUMERIC NOT NULL,
    UNIQUE (symbol, timeframe, datetime)
);

CREATE INDEX idx_candles_datetime ON candles(datetime);
CREATE INDEX idx_candles_symbol_timeframe ON candles(symbol, timeframe);
