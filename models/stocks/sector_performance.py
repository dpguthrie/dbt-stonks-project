import pandas as pd


PERIODS = {
    '1_DAY': 1,
    '1_MONTH': 30,
    '3_MONTHS': 90,
    '1_YEAR': 365,
    '5_YEAR': 1825
}

def model(dbt, session):
    dbt.config(
        materialized="table",
        packages=["pandas"]
    )

    df = dbt.ref('tpch', 'stg_stocks__history')
    profile = dbt.ref('tpch', 'stg_stocks__summary_profile')

    df = df.join(profile, df['symbol'] == profile['symbol']).to_pandas()

    for col, period in PERIODS.items():
        df[col] = df.groupby('SECTOR')['CLOSE'].pct_change(period)

    cols = ['DATE', 'SECTOR'] + list(PERIODS.keys())
    df = df[cols].drop_duplicates()
    return df
    