import td
import datetime as datetime
import numpy as np
from datetime import date, timedelta
import pandas as pd

def ratio(ticker, days):
    print(ticker)
    full = td.optionChainForPredict(ticker)

    calls = full[0]
    callsData = organizeData(calls)
    # print(callsData)

    puts = full[1]
    putsData = organizeData(puts)
    # print(putsData)

    print(timeFrameCalc(callsData, putsData, days))



def organizeData(data):
    data['Volume'] = data['Volume'].astype(int)
    data['Expiration Date'] = pd.to_datetime(data['Expiration Date'],unit='ms').dt.date
    dataInfo = data.groupby(data['Expiration Date']).agg(['sum', 'mean', 'max'])
    return dataInfo['Volume']

def timeFrameCalc(calls, puts, day):
    return (puts.loc[date.today():date.today()+timedelta(days=day)]['sum'].sum()) / (calls.loc[date.today():date.today()+timedelta(days=day)]['sum'].sum())



if __name__ == "__main__":
    pd.set_option("display.max_columns", None)
    pd.set_option('display.max_rows', None)
    pd.set_option('display.width', None)
    pd.set_option('display.max_colwidth', None)

    # ratio('aal', 14)
    positions = td.positions()
    for position in positions[0]:
        ratio(position, 14)
