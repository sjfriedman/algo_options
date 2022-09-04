
import matplotlib.pyplot as plt

import datetime as dt
import awswrangler as wr
import boto3
boto3.setup_default_session(profile_name="personal")

import os
import pandas as pd
pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', 1000)
pd.set_option('display.colheader_justify', 'center')
pd.set_option('display.precision', 2)

# You can query and say give me the unique (keyword distinct) values in a column
# This would be a common way but you are looking over ALL data which feels inefficient
# so you would consider doing this other ways in your 'SCHEMA' or see if what technology
# you are using has different ways to achieve this faster.
def get_expirations_with_query( symbol, db='options'):
    expirations = wr.athena.read_sql_query(
        sql="select distinct(expiration) from :sym;",
        params={"sym":symbol},
        database=db,ctas_approach=False,max_cache_seconds=86400
    )
    expirations = sorted(expirations['expiration'])
    print(expirations)
    return expirations

# since we are using AWS S3 and Glue we built the data with two partitions
# -- partitions are a way to organize data making it faster to look things up later
# our partitions are Expiration and Put_Call, so we can just get the list of
# partitions and return them.
def get_expirations_by_index( symbol, db='options'):
    partitions = wr.catalog.get_csv_partitions(database="options", table=symbol)
    expirations=[]
    for idx in partitions:
        if partitions[idx][1] == 'C':
            expirations.append(partitions[idx][0])
    expirations = sorted(list(set(expirations)))
    print(expirations)
    return expirations

def query_expiration_calls( symbol, date, db='options' ):
    q = wr.athena.read_sql_query(
        "select * from "+symbol+" where expiration= date ':exp;' and put_call='C'",
        params={"exp":date},
        database=db,ctas_approach=False,max_cache_seconds=600
    )
    print(q[q['strike']==q['strike'].median()].sort_values(by='date'))
    return q

def get_table( symbol, db = 'options' ):
    symbol = symbol.upper()
    t = wr.athena.read_sql_table(table=symbol,database=db, ctas_approach=False, max_cache_seconds=600)
    print(t.info())

def delete_table( symbol ):
    wr.catalog.delete_table_if_exists(database="options",table=symbol)

def list_tables(db='options'):
    f = wr.catalog.tables(database="options")
    print(f['Table'])
    return f['Table']

def find_and_delete_partition( symbol, expiration, delete=False ):

    symbol = symbol.upper()
    ### Following is example code how to iterate partitions and clean up data.
    # Get a list of tables in the options DB. each Table is a ticker
    catch = []

    partitions = wr.catalog.get_partitions(database="options", table=symbol)
    for p in partitions:
        if partitions[p][0] == expiration:
            catch.append(partitions[p])
    print( catch )
    # CAREFUL - Delete's parititions!
    if delete is True:
        print ("CLEANING UP")
        wr.catalog.delete_partitions(database="options",table=symbol, partitions_values=catch )

        for f in wr.s3.list_objects('s3://rockfinance/'+symbol+'/'):
            if expiration in f:
                print(os.path.dirname(f))
                p = os.path.dirname(f)
                wr.s3.delete_objects(path=p)


# For testing
if __name__ == "__main__":
    db = 'options'
    symbol='pltr'
    # list_tables(db)
    # delete_table(symbol)
    # get_table(symbol,db)
    # e = get_expirations_with_query(symbol)
    # print( e.iloc[0]['expiration'])
    # query_expiration( symbol, e.iloc[0]['expiration'] )
    # query_expiration_calls( symbol, '2022-01-21' )
    # find_and_delete_partition( "AI", expiration="2020-06-19", delete=True)
    # get_expirations_by_index( symbol )
