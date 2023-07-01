import asyncio
import aiohttp
import zipfile
import pandas as pd
from datetime import datetime, timedelta
import re

WRAPPED_TOKEN_DICT = {"WETH": "ETH", "WLTC": "LTC", "renDOGE": "DOGE"}

def convert_wrapped_token(ticker):
    if ticker in WRAPPED_TOKEN_DICT:
        return WRAPPED_TOKEN_DICT[ticker]
    else:
        return ticker

def extract_price_type(word):
    #for considering the decimal prices here
    match = re.match(r'^([\d.]+)(\D+)', word)
    if match:
        return match.groups()
    else:
        return None

def is_expired(expiration_date):
    return expiration_date >= (datetime.now()- timedelta(days=2)).strftime("%Y-%m-%d")


def convert_expiration_date(date_str):

    # Convert the string to a datetime object
    datetime_obj = datetime.strptime(date_str, "%d-%B-%Y")

    # Convert the datetime object to the desired format
    formatted_date = datetime_obj.strftime("%Y-%m-%d")

    return formatted_date

def process_symbol(s):
    tmp=s.replace(" USDC Collateral","").split()
    tmp[0]=tmp[0][:-4]
    tmp[1]=convert_expiration_date(tmp[1])
    (strike,instrument)=extract_price_type(tmp[2])
    tmp[2]=strike
    tmp.append(instrument)
    return tmp

def calculate_payoff(df):
    if df["SettlementPrice"] == "NaN":
        return "NaN"
    if df["Instrument"]=="Call":
        return max(df["SettlementPrice"]-float(df["Strike"]),0) * df["contractAmount"]
    else:
        return max(float(df["Strike"])-df["SettlementPrice"],0) * df["contractAmount"]

def calculate_profit(df):
    if df["SettlementPrice"] == "NaN":
        return "NaN"
    return float(df["Payoff"])-float(df["premiumPaid"])

def get_unix_time(date_str):
    import numpy as np
    # Parse the input date string
    date = datetime.strptime(date_str, '%Y-%m-%d')

    # Set the time to UTC 08:00
    date = date.replace(hour=8, minute=0, second=0)

    # Convert the datetime object to Unix timestamp
    unix_timestamp = np.int64((date - datetime(1970, 1, 1)).total_seconds()*1000)

    return unix_timestamp

async def download_and_extract_to_csv(ticker,date,granularity="1h"):

    # date should be of the form 'y-m-d'
    if is_expired(date):
        return
    url = f"https://data.binance.vision/data/spot/daily/klines/{ticker}USDT/{granularity}/{ticker}USDT-{granularity}-{date}.zip"
    
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            data_file_prefix=f"data/{ticker}USDT-{granularity}-{date}"
            if response.status == 200:
                data = await response.read()
                
                with open(f"{data_file_prefix}.zip", "wb") as file:
                    file.write(data)
                with zipfile.ZipFile(f"{data_file_prefix}.zip", "r") as zip_ref:
                    zip_ref.extractall(f"{data_file_prefix}")
                
                print(f"Downloaded and unzipped {data_file_prefix}.zip")
            else:
                print(f"Failed to download {data_file_prefix}.zip")
                print(f"url: {url}")
            

                