import math
import datetime
from datetime import date
from yahoo_fin import stock_info as si
from yahoo_earnings_calendar import YahooEarningsCalendar
import pandas as pd
import numpy as np
import scipy.stats as st
import td
import singleStockFunctions
import day_cache as cache



# TIME/DATE INFO
def optionsDataAll(ticker, quantity, tickerTrue, priceTrue, chartTrue, sigmaTrue):
# EARNINGS - Check if we are within 7 days of earnings.
    today = datetime.date.today()
    if singleStockFunctions.getEarningsDate(ticker, today) == False:
        print(ticker, "Earning coming up, skipping")
        return None

# Get stock current price.
    price = singleStockFunctions.getYahooLivePrice(ticker)
    if price == None:
        print(ticker, "No price found, skippping")
        return None

# Days until this Friday expiration?
    daysTill = singleStockFunctions.getDaysTillExpiration(today)

# TICKER INFORMATION
    interval = today - datetime.timedelta(days=31)
    dailyData = si.get_data(ticker, start_date = interval)
    normalData = pd.DataFrame(dailyData, columns = ['adjclose'])

# Determining Sigma
    change = normalData.diff()
    meanChange = change.mean()
    changePercentAbs = round(abs(((change/normalData) * 100)).mean(),2)
    changePercentMean = round(((change/normalData) * 100).mean(),2)


# DETERMINING VOLATILITY/IncreasingDaysTill
    volatilityWarning = singleStockFunctions.volatilityWarningMessage(changePercentAbs)[0]
    daysTill += singleStockFunctions.volatilityWarningMessage(changePercentAbs)[1]

# OPTIONS
    ticker_calls = td.optionChain(ticker, price)
    if ticker_calls.empty:
        print(ticker, "Skipping, empty option chain," )
        return None

# DOWNWARDS PROTECTION
    strikePrice = pd.DataFrame(ticker_calls, columns = ['Strike'])
    optionPrice = pd.DataFrame(ticker_calls, columns = ['Price'])
    protection = price - optionPrice
    ticker_calls['Downwards Protection'] = protection
# SIGMA AND Z SCORES TO PROBABILTITY CONVERSION
    sigma = (math.sqrt(((1 / normalData.shape[0]) * ((meanChange - change)**2)).sum()) / math.sqrt(normalData.shape[0])) * daysTill

    z_ScoreHigh = (strikePrice - price) / sigma
    probabilityHigh = st.norm.cdf(z_ScoreHigh) * 100
    ticker_calls['probability not selling'] = probabilityHigh


    z_ScoreLow = (protection-price) / sigma
    probabilityLow = (st.norm.cdf(z_ScoreLow) * 100)

    combineProb = probabilityHigh - probabilityLow
    ticker_calls['Best Interval No Sell'] = combineProb


# DETERMING BEST STOCK
    bestRow = singleStockFunctions.getBestRow(combineProb)
    if bestRow == None:
        return None


# Checking Volume
    if singleStockFunctions.checkVolume(ticker_calls, bestRow, ticker) == None:
        return None

# CONTRACTS TO BUY
    contracts = (quantity - quantity%100)/100

# OUTPUT
    singleStockFunctions.output(tickerTrue, priceTrue, chartTrue, sigmaTrue, ticker, price, sigma,ticker_calls)

# RETURN
    return [ticker, contracts, price,ticker_calls.loc[bestRow]['Strike'], ticker_calls.loc[bestRow]['Price'],volatilityWarning, ticker_calls.loc[bestRow]['Contract Name'], ticker_calls['Volume'][bestRow] ]

def visual(info, showGains):
    visualInfo = pd.DataFrame(columns = ['Ticker','Volatility','Suggest Day'])
    visualInfo['Ticker'] = info['Ticker']
    visualInfo['Volatility'] = info['Volatility']

    i = 0
    for ticker in visualInfo['Volatility']:
        if ticker == 'Low Volatility':
            visualInfo['Suggest Day'][i] = "12"
        elif ticker == 'Slightly Volatile':
            visualInfo['Suggest Day'][i] = "123:2"
        else:
            visualInfo['Suggest Day'][i] = "234:3"
        i+=1
    if showGains:
        print("Total Gain:", round((info['Purchase Price']*info['Contracts To Buy']).sum()* 100,2))


# For testing
if __name__ == "__main__":
    pd.set_option("display.max_columns", None)
    pd.set_option('display.max_rows', None)
    pd.set_option('display.width', None)
    pd.set_option('display.max_colwidth', None)
    optionsInfo = optionsDataAll("aapl", 100, True, True, True, True )
    # optionsInfo = optionsDataAll("MORT", 100, True, True, True, True )
    print(optionsInfo)
