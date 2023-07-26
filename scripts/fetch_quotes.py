import requests
import pandas as pd
import json
import time
from utils.utils import fmt_date_from_unix, dashed_print, inline_print
import psycopg2 as pg2
from urllib.parse import urlparse
from dotenv import load_dotenv
import os
load_dotenv()

DBPARAMS = os.getenv("TESTDBPARAMS")

p = urlparse(DBPARAMS)

pg_connection_dict = {
    'dbname': p.hostname,
    'user': p.username,
    'password': p.password,
    'port': p.port,
    'host': p.scheme
}

# conn = pg2.connect(
#     dbname="optionsdata",
#     user="openoptions",
#     password="openoptions",
#     host="localhost",
#     port="5234"  # default postgres port
# )

URL = os.getenv("URL")

COLUMNS = ["underlyer", "spot_ref", "expiration_date", "contract_type",
               "strike", "iv", "delta", "theta", "vega", "indic", "description", "quote_time"]
TABLENAME="aevo"

def fetch_quotes(url, logs=True, mock=False):

    if mock:
        with open("./otcdata.json") as f:
            mockdata = json.load(f)
            mockdata["quote_time"] = int(time.time())
            if logs:
                print(mockdata)
        return mockdata

    response = requests.get(url)

    # if the GET request is successful, the status code will be 200
    if response.status_code == 200:
        data = response.json()  # convert response to JSON format
        
        if logs:
            print(data)  # print the data
    else:
        print(f"Request failed with status code {response.status_code}")
    
    res= response.json()
    res["quote_time"] = int(time.time())
    
    return res

def parse_quotes(raw_data, columns, to_df=True):

    quote_time_unix = raw_data['quote_time']

    expires_unix = raw_data['expiries']
    quotes_data = raw_data['data']

    contracts = []

    for underlyers in quotes_data:

        underlyer = underlyers["underlyer"]

        spot_ref = underlyers["spot_ref"]

        for expiration_date, _ in expires_unix.items():

            cur_expiration_quotes = underlyers['prices'][expiration_date]

            for contract_type in ["call", "put"]:

                for contract in cur_expiration_quotes[contract_type]:

                    contract["contract_type"] = contract_type

                    contract["underlyer"] = underlyer

                    contract["spot_ref"] = spot_ref

                    contract["expiration_date"] = expires_unix[expiration_date]

                    contract["quote_time"] = quote_time_unix

                    contract["description"] = contract

                    contract["description"] = underlyer + "-" + fmt_date_from_unix(contract["expiration_date"]) + "-" + \
                        str(contract["strike"]) + "-" + \
                        ("C" if contract_type == "call" else "P")

                    contracts.append(contract)


    if to_df:
        contracts = pd.DataFrame(contracts, columns=columns)

    
    dashed_print(contracts[:2].T,docs="Parsing Data")

    return contracts

def insert_to_db(contracts, dbparams, validate=True):
    print(dbparams)
    # Connect to database
    conn = pg2.connect(**dbparams)
    
    assert(contracts.columns.tolist() == COLUMNS)
    col_str = ', '.join(contracts.columns.tolist())

    query_insert = f"INSERT INTO {TABLENAME} ({col_str}) VALUES %s ON CONFLICT DO NOTHING"

    # Loop through rows
    with conn.cursor() as cursor:
        for t in contracts.itertuples(index=False, name=None):
            cursor.execute(query_insert % str(t))
        conn.commit()
        inline_print("Finish Intersting Data")


    if validate:
        with conn.cursor() as cursor:
            cursor.execute('SELECT * FROM {};'.format(TABLENAME))
            df = pd.DataFrame(cursor.fetchall(), columns=COLUMNS)
            dashed_print(df[:2].T, docs="Validating data in database")
        
    conn.close()

# raw_data = fetch_quotes(URL, logs=False, mock=True)
# contracts = parse_quotes(raw_data,columns=COLUMNS, to_df=True)
# insert_to_db(contracts, dbparams=pg_connection_dict)
