import json
import pandas as pd

import re
from utils.utils import download_and_extract_to_csv, extract_price_type,\
    process_symbol, get_unix_time, convert_wrapped_token, is_expired, calculate_payoff, calculate_profit
import asyncio
from datetime import datetime
from tabulate import tabulate

# load json file
with open('data/tokenInfo.json') as f:
    data = json.load(f)

# convert json to dataframe
df = pd.json_normalize(data)

# pandas do automatic broadcasting here, just consider the one-dimensional case and wrap with pd.Series
df[["Ticker", "Expiration_date", "Strike", "Instrument"]
   ] = df["name"].apply(lambda x: pd.Series(process_symbol(x)))
df["Ticker"] = df["Ticker"].apply(lambda x: convert_wrapped_token(x))

df.drop('name', axis=1, inplace=True)
df.drop('symbol', axis=1, inplace=True)

tickers_info = df[["Ticker", "Expiration_date"]].values.tolist()

granularity = "1h"

tickers_info = [ticker+["1h"] for ticker in tickers_info]


async def get_tickers_csv(tickers_info):

    tasks = []
    for ticker_info in tickers_info:
        task = asyncio.ensure_future(download_and_extract_to_csv(*ticker_info))
        tasks.append(task)

    await asyncio.gather(*tasks)


# Run the asyncio event loop
asyncio.run(get_tickers_csv(tickers_info))


def get_reference_price(tickers_info):
    # filter unexpired options
    reference_prices = []
    for ticker_info in tickers_info:

        (ticker, date, granularity) = ticker_info
        if is_expired(date):
            reference_prices.append("NaN")
            continue
        # read from csv file and return the spot price at UTC 08:0:00 that day
        file_prefix = f"{ticker}USDT-{granularity}-{date}"
        with open(f"data/{file_prefix}/{file_prefix}.csv") as f:
            # extract row with timestamp 08:00:00
            # https://www.binance.com/en/landing/data
            # 0 is open_tme, 4 is Close Price
            df = pd.read_csv(f, header=None, usecols=[0, 4], names=[
                             "timestamp", "close_price"])
            expired_time = get_unix_time(date)
            # return value is a list (cause there might be multiple matched cases)
            reference_price = df[df["timestamp"] ==
                                 expired_time]["close_price"].values[0]
            reference_prices.append(reference_price)
    return reference_prices


ref_prices = get_reference_price(tickers_info)

df["SettlementPrice"] = ref_prices


df["Payoff"] = df.apply(calculate_payoff, axis=1)
df["NetProfit"] = df.apply(calculate_profit, axis=1)

df = df.sort_values('Expiration_date', ascending=False)
# save to csv
df.to_csv("result/TokenInfo_processed.csv", index=False)

print(tabulate(df, headers='keys',tablefmt='psql',showindex="never"))