# Lambda Capture is used to run on a single symbol and fetch end of day options data
# Using S3/Glue it will write the data to the partition [Expiration, Put_Call]
# This gives us history for an expiration date over time.

from yahoo_fin import options as yf
import datetime as dt
import sys

import pandas as pd
pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', 1000)
pd.set_option('display.colheader_justify', 'center')
pd.set_option('display.precision', 2)

import awswrangler as wr
import boto3

# This is a custom clean up process for the source we are using.
# We need to do this to capture only the data we care about and create a standard
# view of the data.
def _clean_up(sym,df,type):
    try:
        if df is None:
            return None
        df.drop(['contract name', 'last trade date', '% change'],axis=1,inplace=True)
        df.rename(columns={'last price':'price','implied volatility':'implied_vol', 'open interest':'open_interest'},inplace=True)
        df.replace(to_replace='^[-]$',value=0,regex=True,inplace=True)
        df.replace(to_replace='[+%,]',value='',regex=True,inplace=True)
        df['symbol'] = sym.upper()
        df['volume'] = pd.to_numeric(df['volume'])
        df['open_interest'] = pd.to_numeric(df['open_interest'])
        df['implied_vol'] = pd.to_numeric(df['implied_vol'])/100
        df['put_call'] = type
        df['date'] = dt.date.today()
    except TypeError as e:
        print(df)
        raise(e)
    return df

# Just get the list of currently known expirations.
def expirations( symbol ):
    try:
        data = yf.get_expiration_dates(symbol)
        iso_dates = []
        for date in data:
            date = dt.datetime.strptime(date,r"%B %d, %Y").date()
            iso_dates.append(date)
        data = iso_dates
        return data
    except:
        return None

# Get all the call data.
def calls( symbol, expiration_date ):
    try:
        data = yf.get_calls(symbol, expiration_date)
        data.columns = data.columns.str.lower()
        data['expiration'] = expiration_date
        return data
    except Exception as e:
        print(expiration_date, e)
        return None

# Get all the pull data
def puts( symbol, expiration_date ):
    try:
        data = yf.get_puts(symbol, expiration_date)
        data.columns = data.columns.str.lower()
        data['expiration'] = expiration_date
        return data
    except Exception as e:
        print(expiration_date, e)
        return None

# Wraps the other calls so we get the full data sets
def capture(sym):
    dates = expirations(sym)
    df = pd.DataFrame()

    for date in dates:
        df = df.append( _clean_up(sym, calls(sym,date),'C'),ignore_index=True)
        df = df.append( _clean_up(sym, puts(sym,date),'P'),ignore_index=True)

    cols = ['date','symbol','expiration','strike', 'put_call', 'bid','ask','price','volume','open_interest','implied_vol']
    return dates, df[cols]

# Write to S3/Glue table structure
def write_table(symbol,df,db):
    wr.s3.to_csv(
        df,
        "s3://rockfinance/"+symbol.upper(),
        index=False,
        dataset=True,
        mode="overwrite_partitions",
        database=db,
        dtype={'date': 'date','expiration': 'date'},
        partition_cols =["expiration","put_call","date"],
        table=symbol.upper())

def lambda_handler(event, context):

    # Get file we are reading.
    symbol=event['Records'][0]["body"]
    print (symbol)

    expirations, df = capture(symbol)
    print(expirations)
    write_table( symbol, df, "options" )
    print( df.head() )


# This is a test function but can be used to mannually run a daily if needed.
# Step 1 - Get source data : capture(symbol)
# Step 2 - Write to S3/Glue as CSV:  write_Table( symbol, data frame, 'options' dB)
if __name__ == "__main__":

    # this will find a local aws profile to figure where to connect
    boto3.setup_default_session(profile_name="personal")

    # Get a list of tables in the options DB. each Table is a ticker
    f = wr.catalog.tables(database="options")
    symbol = f['Table'].iloc[1]
    symbol = "xom"
    print(symbol)

    # This pull data from source to get current available expirations and data.
    expirations, df = capture(symbol)
    print(expirations)
    print(df)
    print(df.info())

    # Write this to S3/Glue Tables
    # write_table(symbol, df, "options" )
