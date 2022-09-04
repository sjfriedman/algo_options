# Define methods that wrap calls to Yahoo and provide automatic caching of historical
# Data.  The goal is to build internal representation and database of data.
# Typically we will store in cache but also build DATA objects.

import os
import datetime as dt
import pandas as pd
from dateutil.parser import parse

import day_cache as cache
from yahoo_fin import stock_info as si

# Load up a stock history using and setting cache when needed
def price_history( symbol, start, end ):

    cache_key = "history."+start.strftime("%Y%m%d")+"-"+end.strftime("%Y%m%d")
    data = cache.get(symbol,cache_key)
    if data is None:
        # Check if data has our date range
        data = si.get_data(symbol, start, end)
        cache.set(symbol,cache_key,data)
    return data

# Gets earning date
# @return earning date as epoch time
def next_earnings_date(symbol):
    try:
        data = quote_data(symbol)
        date = pd.Timestamp(data['earningsTimestamp'][0],unit='s').date()
        return date
    except:
        None

# For a give date check if symbol had earnings coming up.
def earnings_for_date( date, field='data'):

    if isinstance(date, dt.date) or isinstance(date, dt.datetime):
        date = date.strftime("%m-%d-%Y")

    cache_key = "earnings."+date
    data = cache.get("_ALL",cache_key)
    if data is None:
        data = si.get_earnings_for_date(date)
        tickers = []
        for item in data:
            tickers.append(item['ticker'].upper())
        data = {'data':data,'tickers':tickers}
        cache.set("_ALL",cache_key,data)
    return data[field]

# For a give date check if symbol had earnings coming up.
def check_earnings_on_date( symbol, date ):

    if isinstance(date, dt.date) or isinstance(date, dt.datetime):
        date = date.strftime("%m-%d-%Y")

    symbol = symbol.upper()
    try:
        data = earnings_for_date(date,'tickers')
        return symbol in data
    except Exception as e:
        print( "Failed on Date", date )
        raise e


def price(symbol):
    try:
        data = si.get_live_price(symbol)
        return data
    except:
        return None

def quote_table(symbol):
    try:
        data = cache.get(symbol,'quote_table')
        if data is None:
            data = si.get_quote_table(symbol)
            data = pd.DataFrame.from_dict(data,orient='index')
            data = data.T
            data['idx'] = dt.date.today()
            data.set_index('idx', inplace = True)
            cache.set(symbol,'quote_table',data)
        return data
    except:
        return None

def quote_data(symbol):
    try:
        data = cache.get(symbol,'quote_data')
        if data is None:
            data = si.get_quote_data(symbol)
            data = pd.DataFrame.from_dict(data,orient='index')
            data = data.T
            data['idx'] = dt.date.today()
            data.set_index('idx', inplace = True)
            cache.set(symbol,'quote_data',data)
        return data
    except:
        return None

def stats(symbol):
    try:
        data = cache.get(symbol,'stats')
        if data is None:
            data = si.get_stats(symbol)
            # Manipulate DF to have by date recorded
            data.set_index('Attribute', inplace = True)
            data = data.T
            data['idx'] = dt.date.today()
            data.set_index('idx', inplace = True)
            cache.set(symbol,'stats',data)
        return data
    except:
        return None

def stats_valuation(symbol):
    try:
        data = cache.get(symbol,'stats_valuation')
        if data is None:
            data = si.get_stats_valuation(symbol)
            data.rename(columns={'Unnamed: 0':'Attribute'}, inplace = True)
            data.set_index( 'Attribute', inplace = True)
            data.columns = data.columns.str.replace('As of Date: ', '').str.replace('Current','')
            data = data.T
            print(data.index)
            cache.set(symbol,'stats_valuation',data)
        return data
    except e:
        print(e)
        return None
        # stats_valuation = None
        # cache.set(ticker,'stats_valuation',False)

# TODO Data is a Dict with {EPS Revisions, EPS Trend, Earnings Estimate, Earnings History, Revenue Estimate}
def _analysts_info(symbol, type):
    try:
        data = cache.get(symbol,'analysts_info')
        # data = None
        if data is None:
            data = si.get_analysts_info(symbol)
            # return data[type]
            for key, df in data.items():
                df.set_index( key, inplace = True)
                df = data[key].T
                if key in ['Earnings Estimate','Revenue Estimate','EPS Trend','EPS Revisions']:
                    df['period'] = df.index.str.replace(')','')
                    df[['period','timeframe']] = df['period'].str.split('(',n=1,expand=True)
                elif key == "Growth Estimates":
                    df.drop(['Industry','Sector(s)','S&P 500'], axis=0, inplace=True)
                    df['category'] = df.index
                else:
                    df['quarter'] = df.index
                df['idx'] = dt.date.today()
                df.set_index( 'idx', inplace = True)
                data[key] = df
            cache.set(symbol,'analysts_info',data)
        return data[type]
    except:
        return None

def eps_revisions(symbol):
    return _analysts_info(symbol,'EPS Revisions')

def eps_trend(symbol):
    return _analysts_info(symbol,'EPS Trend')

def earnings_estimate(symbol):
    return _analysts_info(symbol,'Earnings Estimate')

def earnings_history(symbol):
    return _analysts_info(symbol,'Earnings History')

def revenue_estimate(symbol):
    return _analysts_info(symbol,'Revenue Estimate')

def growth_estimate(symbol):
    return _analysts_info(symbol,'Growth Estimates')

def income_statement(symbol, annual = True ):
    return None
def balance_sheet(symbol, annual = True):
    return _financials( symbol, ("yearly" if annual else "quarterly") + "_balance_sheet" )
def cash_flow(symbol, annual = True):
    return _financials( symbol, ("yearly" if annual else "quarterly") + "_cash_flow" )

def _financials(symbol,type):
    try:
        data = cache.get(symbol,'financials')
        # data = None
        if data is None:
            data = si.get_financials(symbol)
            for key, df in data.items():
                df = df.T
                if key.startswith("yearly"):
                    df['endYear'] = df.index
                else:
                    df['endQuarter'] = df.index
                df['idx'] = dt.date.today()
                df.set_index('idx', inplace = True)
                data[key]=df
            cache.set(symbol,'financials',data)
        return data[type]
    except:
        return None

def getEarningsHistory(sym):
    earnings = si.get_earnings_history(sym)
    dates = pd.DataFrame(columns = {'Earnings'})
    for date in earnings:
        dates.loc[len(dates.index)] = parse(date['startdatetime']).date()
    return dates


# For testing
if __name__ == "__main__":
    pd.set_option('display.max_rows', None)
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', 1000)
    pd.set_option('display.colheader_justify', 'center')
    pd.set_option('display.precision', 2)

    sym = "aapl"
    import pprint
    pp = pprint.PrettyPrinter(indent=4)

    # pp.pprint(check_earnings_on_date(sym, "01-20-2021") )
    # pp.pprint(earnings_for_date( "01-20-2021") )
    # pp.pprint(earnings_for_date( "01-20-2021", 'tickers') )
    #
    # pp.pprint( cash_flow(sym, True))
    # pp.pprint( cash_flow(sym, False))
    # pp.pprint( balance_sheet(sym, True))
    # pp.pprint( balance_sheet(sym, False))
    # pp.pprint( income_statement(sym, True))
    # pp.pprint( income_statement(sym, False))
    # pp.pprint( eps_revisions(sym))
    # pp.pprint( eps_trend(sym))
    # pp.pprint( earnings_estimate(sym))
    # pp.pprint( revenue_estimate(sym))
    # pp.pprint( earnings_history(sym))
    # pp.pprint( growth_estimate(sym))
    # print(sym,next_earnings_date(sym))
    # print(sym,price(sym))
    # pp.pprint(quote_table(sym))
    # pp.pprint(quote_data(sym))
    # pp.pprint(stats(sym))
    # pp.pprint(stats_valuation(sym))
    print(getEarningsHistory(sym))
