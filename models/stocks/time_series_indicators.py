import numpy as np


SMA_LIST = [50, 100]
EMA_LIST = [12, 20, 26]
RSI_LIST = [9, 14, 25]

def calculate_rsi(series, period):
    delta = series.diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
    rs = gain / loss
    return 100 - (100 / (1 + rs))


def stochastic_oscillator(df, period=14):
    low_min = df['LOW'].rolling(window=period).min()
    high_max = df['HIGH'].rolling(window=period).max()
    return 100 * ((df['CLOSE'] - low_min) / (high_max - low_min))


def model(dbt, session):
    dbt.config(
        materialized="table",
        packages=["numpy", "pandas"],
        snowflake_warehouse="SNOWPARK_WH",
    )
    df = dbt.ref('tpch', 'stg_stocks__history').to_pandas()
    df.sort_values(by=['SYMBOL', 'DATE'], inplace=True)
    
    df['DAILY_RETURN'] = df.groupby('SYMBOL')['CLOSE'].pct_change()

    # Simple Moving Averages (SMA)
    for n in SMA_LIST:
        df[f'SMA_{n}'] = df.groupby('SYMBOL')['CLOSE'].rolling(
            window=n
        ).mean().reset_index(level=0, drop=True)

    #     # Bollinger Bands
        df[f'STD_{n}'] = df.groupby('SYMBOL')['CLOSE'].rolling(
            window=20
        ).std().reset_index(level=0, drop=True)
        df[f'UPPER_BAND'] = df[f'SMA_{n}'] + (df[f'STD_{n}'] * 2)
        df[f'LOWER_BAND'] = df[f'SMA_{n}'] - (df[f'STD_{n}'] * 2)

    # # Exponential Moving Averages (EMA)
    for n in EMA_LIST:
        df[f'EMA_{n}'] = df.groupby('SYMBOL')['CLOSE'].ewm(
            span=n, adjust=False
        ).mean().reset_index(level=0, drop=True)

    # # Relative Strength Index (RSI)
    for n in RSI_LIST:
        df[f'RSI_{n}'] = df.groupby('SYMBOL')['CLOSE'].apply(
            calculate_rsi, period=n
        ).reset_index(level=0, drop=True)

    # # Stochastic Oscillator
    df['STOCHASTIC_OSCILLATOR'] = df.groupby('SYMBOL').apply(
        stochastic_oscillator
    ).reset_index(level=0, drop=True)

    return df
