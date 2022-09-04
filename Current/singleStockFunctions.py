import math
import datetime
from datetime import date
from yahoo_fin import stock_info as si
from yahoo_earnings_calendar import YahooEarningsCalendar
import pandas as pd
import numpy as np
import scipy.stats as st
import td
import day_cache as cache

def getYahooLivePrice(ticker):
    try:
        price = cache.get(ticker,'live_price')
        if price is None:
            price = si.get_live_price(ticker)
            cache.set(ticker,'live_price',price)
        return price
    except:
        return None

def getEarningsDate(ticker, todayDate):
    earningsDate = calendar.get_next_earnings_date(ticker)
    earningsBoolean = True
    if earningsDate:
        if (earningsDate - todayDate).days < 7:
            earningsBoolean = False
    return earningsBoolean

def getDaysTillExpiration(friday):
    today = friday
    while friday.strftime('%a') != 'Fri':
        friday += datetime.timedelta(1)
    daysTill = (friday - today).days+1

    # If it's after 4pm
    current_time = datetime.datetime.now().time()
    if current_time.hour > 16 :
        daysTill = (friday - today).days

    if (today.weekday()>=5) or (today.weekday == 4 and current_time.hour > 16):
        daysTill = 5
    return daysTill

def volatilityWarningMessage(changePercentAbs):
    volatilityWarning = ""
    daysTillIncrease = 0
    if(changePercentAbs[0] < 1):
        volatilityWarning = "Very Low Volatility"
        daysTillIncrease+=0.5
    elif changePercentAbs[0] >= 1 and changePercentAbs[0] < 2.5:
        volatilityWarning = "Low Volatility"
        daysTillIncrease+=1
    elif changePercentAbs[0] >= 2.5 and changePercentAbs[0] < 4:
        volatilityWarning = "Volatile"
        daysTillIncrease+=2
    elif changePercentAbs[0] >= 4 and changePercentAbs[0]<6:
        volatilityWarning = "High Volatility"
    else:
        volatilityWarning = "Extreme Volatility"
    return volatilityWarning, daysTillIncrease

def getBestRow(combineProb):
    i = 0
    bestRow = -1
    bestProb = 0
    for rows in combineProb:
        if rows >= combineProb[bestRow][0]:
            if round(rows[0],2) == round(combineProb[bestRow][0],2) and i!=0:
                break
            bestRow = i
            bestProb = rows
        elif bestRow != -1:
            break

        i+=1
    if bestRow == -1:
        print(ticker,"Did not reach best probability")
        return None
    return bestRow

def checkVolume(ticker_calls, bestRow, ticker):
    if (float)(ticker_calls['Volume'][bestRow]) < 25:
        print(ticker, "No Volume")
        return None
    return True

def output(tickerTrue, priceTrue, chartTrue, sigmaTrue, ticker, price, sigma,ticker_calls):
    if tickerTrue:
        print(ticker, "Processed")
    if priceTrue:
        print("Price",price)
    if sigmaTrue:
        print("Sigma:", sigma)
    if chartTrue:
        print(ticker_calls)
