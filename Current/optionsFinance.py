import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))+"/Common")
import td

import math
import datetime
from yahoo_fin import stock_info as si
from yahoo_fin import options
import pandas as pd
import numpy as np
import scipy.stats as st
import singleStock

def getOptionableStocks(showTicker=False):
    # ACCESS TO TD SERVERS
    _positions = td.positions()
    # posOptionable = ['MCD', 'MO', 'TXN', 'HYG', 'XLI', 'LUV', 'XLY', 'SPY', 'AMT', 'CVX', 'AAL', 'IWC', 'XLK', 'NVDA', 'CRM', 'CSCO', 'FTOC', 'IBM', 'SBUX', 'NET', 'ADP', 'V', 'HON', 'BIP', 'XLP', 'PLTR', 'AMD', 'HYD', 'AI', 'QCOM', 'PLD', 'D', 'PEP', 'MRK', 'WFC', 'FIS', 'AAPL', 'XOM', 'HD', 'VTRS', 'SPLK', 'PINS', 'LULU', 'MSFT', 'WORK', 'VZ', 'PFE', 'FB', 'PYPL', 'T', 'ABNB', 'TGT', 'ABBV']
    posOptionable = _positions[0]
    posQty = _positions[2]
    # posQty = 100

    optionsInfo = []
    availableStocks = pd.DataFrame(columns = ['Ticker', 'Contracts To Buy', 'Price', 'Strike', 'Purchase Price', 'Volatility', 'Contract Name', 'Volume'])

    # order for singleStock
    # ticker, quantity, showTicker, showPrice, showChart, showSigma
    i = 0
    for ticker in posOptionable:
        optionsInfo = singleStock.optionsDataAll(ticker, posQty[i], showTicker, False, False, False)
        i+=1
        if optionsInfo:
            availableStocks = availableStocks.append({'Ticker' : optionsInfo[0],
                                                    'Contracts To Buy' : optionsInfo[1],
                                                    'Price' : optionsInfo[2],
                                                    'Strike' : optionsInfo[3],
                                                    'Purchase Price' : optionsInfo[4],
                                                    'Volatility' : optionsInfo[5],
                                                    'Contract Name' : optionsInfo[6],
                                                    'Volume' : optionsInfo[7]} ,ignore_index=True)

    return availableStocks


# For testing
if __name__ == "__main__":

    import os
    from dotenv import load_dotenv
    load_dotenv()
    save_orders = os.getenv("save_orders", False)

    print( "Starting to analyze your account.")
    optionableStocks = getOptionableStocks(True)

    pd.set_option("display.max_columns", None)
    pd.set_option('display.max_rows', None)
    pd.set_option('display.width', None)
    pd.set_option('display.max_colwidth', None)

    if optionableStocks.empty:
        print("No stocks for options")
    elif False:
        print( "\nOptionable Stocks")
        print(optionableStocks)
    else:
        print( "\nOptionable Stocks")
        print(optionableStocks.to_csv())


    print("\nVisual")
    singleStock.visual(optionableStocks, True)

    if save_orders == "True":
        td.optionsToBuy(optionableStocks)
