import json
import boto3
import urllib.parse
import pandas as pd
import awswrangler as wr

# Global S3 Client
s3 = boto3.client('s3')

# Simple abstract to read file from either S3 or local path into a DF.
# @return DF
def read_csv( path ):
    if path.startswith("s3"):
        return wr.s3.read_csv(path=[path])
    else:
        return pd.read_csv(path)

# Read the historical file and munge to the data set we want
# See cols= for list of columns stored.
# @return Symbol , DF
def read_option_file( path ):

    df = read_csv(path)

    # clean column names
    df.columns = df.columns.str.strip()

    # Convert date to datetime

    df['date'] = pd.to_datetime(df['date']).dt.date
    df['expiration'] = pd.to_datetime(df['expiration']).dt.date

    # Massage data into format we want to return
    df.columns = df.columns.str.lower()
    df.rename(columns = {'symbol':'contract','under':'symbol', 'put/call':'put_call', 'open interest':'open_interest', 'implied vol':'implied_vol'}, inplace = True)

    cols = ['date','symbol','expiration','strike', 'put_call', 'bid','ask','price','volume','open_interest','implied_vol']
    df = df[cols]

    symbol = df['symbol'].iloc[0].upper()

    return symbol, df

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
        partition_cols =["expiration","put_call", "date"],
        table=symbol.upper())

def lambda_handler(event, context):

    # Get file we are reading.
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    s3file = "s3://"+bucket+"/"+key
    print (s3file)

    symbol, df = read_option_file(s3file)
    write_table( symbol, df, "options" )
    print( df.describe() )

    return {
        'statusCode': 200,
        'body': symbol
    }

# For testing this can either
# --- run the code to parse historical file and write to able.
# --- or load the historical file to S3 and let lambda just kick in.
if __name__ == "__main__":

    boto3.setup_default_session(profile_name="personal")

    import sys
    import os
    sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))+"/Common")
    import utils

    symbol = "aapl"
    # if len(sys.argv) == 2:
    #     symbol = sys.argv[1]
    # path = utils.getBaseDir(symbol)

    # Option 1 - Do the work here.
    df = pd.DataFrame()
    for file in os.listdir(path):
        if file.startswith(symbol+".options."):
            _, f = read_option_file( path + file)
            df = df.append(f, ignore_index=True)

    # gb = df.groupby(by=['date'], axis=0 )
    # for i,f in gb:
    #     write_table( symbol, f, "options" )

    # print ( df.head() )

    # Option 2 - Load to S3
    # for file in os.listdir(path):
    #     if file.startswith(symbol+".options."):
    #         local_file = path+file
    #         s3_path = "s3://rockfinance-source/historical/"+file
    #         wr.s3.upload(local_file=local_file,path=s3_path)

    # Option 3 - Go to S3 and drop in historical file.
