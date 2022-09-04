# To import from our common area we need to use system path TODO: Make real module.
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))+"/Common")
import td
import options

# Import our module files.
import sigma_model as model

# Import packages for data
import numpy as np
import pandas as pd
import datetime as dt

# Import for working with AWS
import awswrangler as wr
import boto3

# We are looking for the best sigma/multiplier for selling call option
# Returns ( BestMultiplier, gains[] )
# OR None if a model could not be evaluated
def get_model( ticker ):

    # Load our data set
    print( "DATA")
    gb, historical_expirations, testing_expirations = get_data_set(ticker)

    result = {}

    # See if we have a best multiplier based on historical data.
    try:
        print("BUILD")
        best_multiplier, best_fit, failed_expirations = build_model(ticker, gb, historical_expirations)
        if best_multiplier is None:
            return None
        best_multiplier = best_multiplier.index[0]
    except Exception as e:
        print( ticker, "MULTI")
        raise e

    # Test this on recent data and see if it holds up.
    try:
        print("TEST")
        value = test_model(ticker, best_multiplier, gb, testing_expirations)
        print(value)
        sum = round(value['gains'].sum(),2)
        return best_multiplier, value['gains'].to_numpy()
    except Exception as e:
        print( ticker, "PREDICT")
        raise e

# Returns the DataFrameGroupBy with two lists
# historical - array of expirations to use for model
# testing - array of expirations to use for testing
# @param test how many weeks to reserve for testing
# @param min min number of expirations for building model
def get_data_set(ticker,min=6,test=2):

    # This will get each weeks of expiration of historical data.
    option_history = options.get_options_by_expiration_eod_final_week(ticker)

    # Group by expirations
    gb = option_history.groupby('expiration',sort=False)

    # create list of historical expirations
    expirations = list(gb.groups.keys())
    if len(expirations) < (min+test):
        raise Exception("Not enought expirations available")
    historical = expirations[:-1*test]
    testing = expirations[-1*test:]

    return gb, historical, testing

# Call the algo for each historical data week iterating over many sigma
# The sum all history by sigma and find one with best gains.
def build_model(ticker, gb, expirations):

    best_fit = pd.DataFrame()
    failed_expirations = []
    best_sigma = None

    for expiration in expirations:
        df = model.build_model(ticker, option_data=gb.get_group(expiration), max_multiplier=3, step=.1)
        if df is None or df.empty:
            failed_expirations.append(expiration)
        else:
            best_fit = best_fit.append(df)

    print( len(expirations), len(failed_expirations))
    print(best_fit.head())
    print(best_fit.tail())

    # Select the one with best gains.
    if best_fit.empty is False:
        summary = best_fit.groupby(['multiplier']).sum()
        best_sigma = summary[summary['gains']==summary['gains'].max()].head(1)
        multiplier = best_sigma.index[0]
        print(multiplier)
        print(best_fit[best_fit['multiplier'] == multiplier])

    return best_sigma, best_fit, failed_expirations

def test_model(ticker, multiplier, gb, expirations ):

    data = pd.DataFrame()
    failed_expirations = []
    for expiration in expirations:
        df = model.test_model(ticker, gb.get_group(expiration), multiplier)
        if df is None or df.empty:
            print("Skipping Test Expiration", expiration)
            failed_expirations.append(expiration)
        else:
            data = data.append(df)

    return data

# Always use a main to run your code.
# Perhaps learn about it @ https://docs.python.org/3/library/__main__.html#module-__main__
if __name__ == "__main__":

    # For printing pd
    pd.set_option("display.max_columns", None)
    pd.set_option('display.max_rows', None)
    pd.set_option('display.width', None)
    pd.set_option('display.max_colwidth', None)

    # this will find a local aws profile to figure where to connect
    boto3.setup_default_session(profile_name="personal")

    # positions = td.positions()
    # ticker = 'net'
    # # Get something?
    weeks = [
        'aapl',
        # 'pltr',
        # 'pfe'"st",
        # 'lulu',
        # 'net'
    ]

    ticker = 'aapl'
    multiplier, pass_fail = get_model(ticker)
    print(multiplier, pass_fail)
    exit()

    # f = wr.catalog.tables(database="options")
    # results = {}
    # for ticker in f['Table']:
    #     multiplier, pass_fail, msg = get_model(ticker)
    #     results['ticker'] = (multiplier, pass_fail, msg)
    #     print(ticker, results['ticker'])
