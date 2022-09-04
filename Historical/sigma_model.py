# So far each model requires
# build_model - uses historical data to craft best model
# test_model - use historical data to test model
# predict_model - use to select best option for data

# To import from our common area we need to use system path TODO: Make real module.
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))+"/Common")
import yahoo as yf

# imports for data anlaysis
import math
import datetime as dt
import pandas as pd
import numpy as np
import scipy.stats as st
import sys

def test_model(ticker, option_data, multiplier):
    multiplier_range = np.arange(multiplier,multiplier+.1,1)
    return algo(ticker, option_data=option_data, multiplier_range=multiplier_range)

def build_model(ticker, option_data, max_multiplier=1, step=1):
    # The range of sigma we want to test.
    multiplier_range = np.arange(1,max_multiplier,step)

    #Call algo to get back best fits.
    return algo(ticker, option_data=option_data, multiplier_range=multiplier_range)

def predict_model( ticker, option_data, multiplier ):
    return


# Convert Volatitily value to string definition
# Very Low, Low, Volatile, High, Extreme
def volatilityWarningMessage(changePercentAbs):
    volatilityWarning = ""
    if changePercentAbs < 1:
        volatilityWarning = "Very Low Volatility"
    elif changePercentAbs < 2.5:
        volatilityWarning = "Low Volatility"
    elif changePercentAbs < 4:
        volatilityWarning = "Volatile"
    elif changePercentAbs < 6:
        volatilityWarning = "High Volatility"
    else:
        volatilityWarning = "Extreme Volatility"
    return volatilityWarning

# Find the best row with probability information
# combineProp should be array[float] for the probabibilties
# ticker - symbol just for output
def getBestRow(combineProb, ticker):
    bestRow = None
    bestProb = 0
    for index, prob in combineProb.items():
        if prob >= bestProb:
            if bestRow and round(prob,2) == round(bestProb,2) :
                break
            bestRow = index
            bestProb = prob
        elif bestRow:
            break

    if not bestRow:
        print(ticker,"Did not reach best probability")
        return None

    return bestRow

# The Algo
# Look at day one of final week
# @param option_week for a given expiration give the final weeks EOD pricing.
def algo(ticker, option_data, multiplier_range=None, min_volume=20):

    # Should give us start day of week
    day_one = option_data['date'].min()

    # Expiration date we are looking at (end of week)
    expiration = option_data['expiration'].min();

    for n in range(5):
        date = (day_one + dt.timedelta(n))
        earnings = yf.check_earnings_on_date(ticker,date)
        if earnings is True:
            print("Skipping for earnings on ", date)
            return None

    # Days until this Friday expiration?
    daysTill = (expiration - day_one).days + 1
    if daysTill <= 0:
        return None

    # Get underlying symbol data from 100 days before week plus the week
    stockPriceData = yf.price_history(ticker, day_one - dt.timedelta(days=31), expiration + dt.timedelta(days=1))
    if stockPriceData.empty:
        return None

    # Get stock current price.
    monday_price = stockPriceData['open'].loc[day_one]
    friday_price = stockPriceData['close'].loc[expiration]

    # Get last 100 days of ticker information before start of week
    # just get Adjusted Close information
    adj_close_stock = stockPriceData[stockPriceData.index<day_one]
    adj_close_stock = adj_close_stock['adjclose']

    # Determining Sigma by looking at the stock price history.
    change = adj_close_stock.diff()
    meanChange = change.mean()
    changePercentAbs = round(abs(((change/adj_close_stock) * 100)).mean(),2)
    # adj_close_stock.shape[0] is the count of elements in the last 100 days data points (not counting non trading days, hence not 100)
    # sigma = SQRT OF ( SUM of ((1/n*)*(mean-change)^2) )  (where n=number of data points)
    sigma = (math.sqrt(((1 / adj_close_stock.shape[0]) * ((meanChange - change)**2)).sum()))

    # DETERMINING VOLATILITY/IncreasingDaysTill
    volatilityWarning = volatilityWarningMessage(changePercentAbs)

    # We only look at OTM for the week.
    option_week = option_data[option_data['strike']>monday_price].copy()

    # DOWNWARDS PROTECTION,  monday - option
    # Adds column Downwards Protection
    protection = monday_price - option_week[option_week['date'] == day_one]['price']
    option_week['Downard Protection'] = protection

    # We iterate on a sigma multiplier supplied in the function all
    # For each iteration we look for the best strike price.
    _columns = ['symbol','monday','friday','strike', 'price',
                'volatile','volume','pass_fail',
                'pf_rate', 'fail_date', 'fail_buy', 'gains',
                'start','sigma','multiplier', 'expiration']
    best_options = pd.DataFrame(columns = _columns )
    for multi in multiplier_range:

        # Z SCORES TO PROBABILTITY CONVERSION
        z_ScoreHigh = ((option_week['strike'] - monday_price) / ( sigma * multi )).dropna()
        probability_high = pd.Series(st.norm.cdf(z_ScoreHigh) * 100,index=z_ScoreHigh.index)
        option_week['probability_high'] = probability_high

        z_ScoreLow = ((-1*option_week['price']) / ( sigma * multi )).dropna()
        probability_low = pd.Series((st.norm.cdf(z_ScoreLow) * 100),index=z_ScoreLow.index)
        option_week['probability_low'] = probability_low

        combine_prob = probability_high - probability_low
        option_week['combine_prob'] = combine_prob

        # DETERMING BEST STOCK
        best_row = getBestRow(combine_prob, ticker)
        best_strike = option_week.loc[best_row]['strike']
        best_price = option_week.loc[best_row]['price']
        # print( round(multi,2), best_row, best_strike, best_price,option_week['volume'].loc[best_row] )

        # Checking Volume
        best_volume = option_week['volume'].loc[best_row]
        if best_volume < min_volume:
            # print( ticker, "Skipping sigma, volume too low", best_volume, best_strike, monday_price, expiration )
            continue;

        # Did our strike price fail at all during the week causing a buy back?
        real_gains = best_price
        over_strike = stockPriceData[( stockPriceData.index>=day_one) & (stockPriceData['close'] > best_strike)]
        if over_strike.empty:
            pass_fail = "PASS"
            pass_fail_rate = round(100 * (best_strike/friday_price-1), 2)
            fail_first_date = None
            fail_buy_back = None
        else:
            pass_fail = "FAIL"
            pass_fail_rate = round(100 * (friday_price/best_strike-1), 2)

            fail_first_date = None
            fail_buy_back = None
            for index in over_strike.index:
                buy_back = option_week[(option_week['date']==index) & (option_week['strike']==best_strike)]
                if buy_back.empty == False:
                    fail_first_date = index
                    fail_buy_back = buy_back["price"].iloc[0]
                    break
            if fail_buy_back is None:
                return None
            real_gains -= fail_buy_back

        best_options = best_options.append(
                {'symbol' : ticker,
                'monday' : monday_price,
                'friday' : friday_price,
                'strike' : best_strike,
                'price' : best_price,
                'volatile' : volatilityWarning,
                'volume' : best_volume,
                'pass_fail' : pass_fail,
                'pf_rate': pass_fail_rate,
                'fail_date': fail_first_date,
                'fail_buy': fail_buy_back,
                'gains' : real_gains,
                'start' : day_one,
                'sigma': sigma,
                'multiplier': multi,
                'expiration' : expiration }, ignore_index=True)

    # RETURN arra[dict with  ticker, monday close price, friday close price]
    return best_options
