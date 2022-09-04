
import awswrangler as wr
import boto3
import datetime as dt

def check_for_data(symbol, date, db='options'):
    df = wr.athena.read_sql_query(
        "select count(*) from :sym; where date = date ':date;'",
        params={"sym":symbol,"date":date},
        database=db,ctas_approach=False
    )
    if df is None:
        return 0
    else:
        return df.iloc[0][0]

def lambda_handler(event, context):

    # Get list of tables, which is each symbol to process.
    f = wr.catalog.tables(database="options")

    # For reach symbol get the list of partitions and make sure we have todays partitions
    results = {}
    failed = False
    yesterday = dt.date.today() - dt.timedelta(days=1)
    for symbol in f['Table']:
        results[symbol] = check_for_data(symbol, yesterday)
        if results[symbol] == 0:
            print (symbol, "Failed to find data")
            failed = True
    print(results)

    if failed == True:
        return "Symbols failed to load data"

# For testing but if you call this with a new symbol that symbol will start tracking daily.
if __name__ == "__main__":

    # this will find a local aws profile to figure where to connect
    boto3.setup_default_session(profile_name="personal")

    ## Simulate the lambda and go through every stock in the db
    # print(lambda_handler( None, None ))

    # ## TO Check single stock for single day.
    # date = '2021-05-03'
    # symbol = 'splk'
    # count = check_for_data(symbol=symbol,date=date)
    # print(count)
    # exit()

    #### Following is example code how to iterate partitions and clean up data.
    # # Get a list of tables in the options DB. each Table is a ticker
    # f = wr.catalog.tables(database="options")
    # count = 0
    # for sym in f['Table']:
    #     partitions = wr.catalog.get_partitions(database="options", table=sym, expression=)
    #     # catch = []
    #     # for p in partitions:
    #     #     if partitions[p][2] == "2021-04-29":
    #     #         count = count + 1
    #     #         print(sym, p, partitions[p][2])
    #     #         catch.append(partitions[p])
    #     #         break
    #     # print( catch )
    #         # CAREFUL - Delete's parititions!
    #         # wr.catalog.delete_partitions(database="options",table=sym, partitions_values=catch )
    # print ("found ", count )
    # # for f in wr.s3.list_objects('s3://rockfinance/AAPL/'):
    # #     if "2021-04-29" in f:
    # #         print(os.path.dirname(f))
    # #         # p = os.path.dirname(f)
    # #         # wr.s3.delete_objects(path=p)
