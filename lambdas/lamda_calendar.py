import awswrangler as wr
import boto3


from yahoo_fin import stock_info as si
from dateutil.parser import parse
import pandas as pd


# Write to S3/Glue table structure
def getEarningsHistory(sym):
    earnings = si.get_earnings_history(sym)
    dates = pd.DataFrame(columns = {'Earnings'})
    for date in earnings:
        dates.loc[len(dates.index)] = parse(date['startdatetime']).date()
    return dates

def create_new_table(symbol,db):
    # wr.s3.delete_objects(path="s3://rockfinance-sam/"+symbol.upper())
    
    wr.s3.to_csv(
        getEarningsHistory(symbol),
        "s3://rockfinance-sam/"+symbol.upper(),
        index=False,
        dataset=True,
        mode="overwrite_partitions",
        database=db,
        dtype={'Earnings': 'date'},
        table=symbol.upper())

# def lambda_handler(event, context):
#
#     # Get file we are reading.
#     symbol=event['Records'][0]["body"]
#     print (symbol)
#
#     expirations, df = capture(symbol)
#     print(expirations)
#     write_table( symbol, df, "stock" )
#     print( df.head() )


# This is a test function but can be used to mannually run a daily if needed.
# Step 1 - Get source data : capture(symbol)
# Step 2 - Write to S3/Glue as CSV:  write_Table( symbol, data frame, 'options' dB)
if __name__ == "__main__":

    # this will find a local aws profile to figure where to connect
    boto3.setup_default_session(profile_name="personal")
    create_new_table('aapl', "stock")


    # Get a list of tables in the options DB. each Table is a ticker
    # f = wr.catalog.tables(database="options")
    # symbol = f['Table'].iloc[1]
    # symbol = "xom"
    # print(symbol)
    #
    # # This pull data from source to get current available expirations and data.
    # expirations, df = capture(symbol)
    # print(expirations)
    # print(df)
    # print(df.info())
