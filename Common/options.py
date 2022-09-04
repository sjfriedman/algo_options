# Real time options data from Yahoo or TD

import datetime as dt
import pandas as pd

import day_cache as cache
from yahoo_fin import options as yf

import awswrangler as wr
import boto3

def expirations( symbol ):
    try:
        data = cache.get(symbol,'expirations')
        if data is None:
            data = yf.get_expiration_dates(symbol)
            iso_dates = []
            for date in data:
                date = dt.datetime.strptime(date,r"%B %d, %Y").date()
                iso_dates.append(date)
            data = iso_dates
            cache.set(symbol,'expirations',data)
        return data
    except:
        return None

def calls( symbol, expiration_date ):
    try:
        data = yf.get_calls(symbol)
        data.columns = data.columns.str.lower()
        data['expiration'] = expiration_date
        data['date'] = dt.date.today()
        return data
    except:
        return None

def puts( symbol, expiration_date ):
    try:
        data = yf.get_puts(symbol)
        data.columns = data.columns.str.lower()
        data['expiration'] = expiration_date
        data['date'] = dt.date.today()
    except:
        return None

# Get all historical data for the week of an expiration for expirations before today.
def get_options_by_expiration_eod_final_week(symbol, db='options', call_or_put = 'C'):

    df = wr.athena.read_sql_query(
        "select * from :sym; "+
        " where "+
        "  expiration < date ':exp;' "+
        "  and date > expiration - interval '5' day  "+
        "  and put_call=':call_or_put;'"+
        " order by expiration, date, strike",
        params={
            "sym":symbol,
            "exp":dt.date.today(),
            "call_or_put": call_or_put
            },
        database=db,ctas_approach=False,max_cache_seconds=86400
    )
    df['expiration'] = pd.to_datetime(df['expiration'], format='%Y-%m-%d')
    df['date'] = pd.to_datetime(df['date'], format='%Y-%m-%d')

    return df


# For testing
if __name__ == "__main__":

    pd.set_option('display.max_rows', None)
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', 1000)
    pd.set_option('display.colheader_justify', 'center')
    pd.set_option('display.precision', 2)

    sym = "ibm"
    dates = expirations(sym)
    for date in dates:
        # fetch = dt.datetime.strptime(date,r"%B %d, %Y").date()
        print(date)
        data = calls(sym, date)
        # data = puts(sym, fetch)
        print(data)
        # exit()
