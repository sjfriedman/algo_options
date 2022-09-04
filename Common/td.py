import os
from dotenv import load_dotenv

import tda
from tda import auth, client
from tda.auth import easy_client
from tda.client import Client

import datetime
import json
import pandas as pd

import day_cache as cache

def __dump(j):
    print(json.dumps(j,indent=2))

def get_client():
    load_dotenv()
    token_path = os.getenv("token_path")
    api_key = os.getenv("api_key")
    redirect_url = os.getenv("redirect_url")

    try:
        c = auth.client_from_token_file(token_path, api_key)
    except FileNotFoundError:
        from selenium import webdriver
        with webdriver.Chrome() as driver:
            c = auth.client_from_login_flow(
                driver, api_key, redirect_url, token_path)
    return c

# Get list of positions for first account returned from TD.
# returns   arr - list of symbols
#           data - full json from TD
#           volume - array of shares.
def positions():
    client = get_client()
    account_selector = (int)(os.getenv('account_selector'))
    r = client.get_accounts(fields=client.Account.Fields.POSITIONS)
    assert r.status_code == 200, r.rause_for_status(positions)
    data = r.json()
    # print(json.dumps(data, indent = 2))
    arr = []
    volume = []
    for pos in data[account_selector]['securitiesAccount']['positions']:
        if pos['longQuantity'] >= 100  and pos['instrument']['assetType'] == 'EQUITY':
            arr.append(pos['instrument']['symbol'])
            volume.append((float)(pos['longQuantity']))
    return arr, data, volume


# Get the option chain for a ticker where strike > price (ie OTM)
# return DataFrame [Strike,Price,ContractName]
def optionChain(ticker, price):
    today = datetime.date.today()
    endInterval = today + datetime.timedelta(days = 7)
    client = get_client()
    optionsJson = cache.get(ticker,'td_options')
    if optionsJson is None:
        options = client.get_option_chain(ticker.upper(),
            contract_type = client.Options.ContractType.CALL,
            include_quotes = True,
            strike_count = 100,
            from_date = today, to_date = endInterval)
        optionsJson = options.json()
        # __dump(optionsJson)
        cache.set(ticker,'td_options',optionsJson)
    optionsMap = optionsJson['callExpDateMap']
    # __dump(optionsMap)
    optionsInfo = pd.DataFrame(columns = ['Strike', 'Price','Contract Name', 'Volume'])
    for date in optionsMap:
        for strike in optionsMap[date]:
            chain = optionsMap[date][strike][0]
            if((float)(strike) > (float)(price)):
                optionsInfo = optionsInfo.append( { "Strike": (float)(strike),
                                "Price": (chain['last']),
                                "Contract Name": chain['symbol'],
                                'Volume' : chain['totalVolume']}, ignore_index=True )
    return optionsInfo

def optionChainForPredict(ticker):
    today = datetime.date.today()
    endInterval = today + datetime.timedelta(days = 365)
    client = get_client()
    # optionsJson = cache.get(ticker,'td_options')
    optionsJson = None
    if optionsJson is None:
        options = client.get_option_chain(ticker.upper(),
            include_quotes = True,
            from_date = today, to_date = endInterval)
        optionsJson = options.json()
        # __dump(optionsJson)
        cache.set(ticker,'td_options',optionsJson)
    optionsMapCall = optionsJson['callExpDateMap']
    optionsMapPut = optionsJson['putExpDateMap']
    # __dump(optionsMap)
    optionsInfoCall = pd.DataFrame(columns = ['Strike', 'Price','Contract Name', 'Volume', 'Type', 'Expiration Date'])
    optionsInfoPut = pd.DataFrame(columns = ['Strike', 'Price','Contract Name', 'Volume', 'Type', 'Expiration Date'])

    for date in optionsMapCall:
        for strike in optionsMapCall[date]:
            chain = optionsMapCall[date][strike][0]
            optionsInfoCall = optionsInfoCall.append( { "Strike": (float)(strike),
                            "Price": (chain['last']),
                            "Contract Name": chain['symbol'],
                            'Volume' : chain['totalVolume'],
                            'Type' : chain['putCall'],
                            'Expiration Date' : (int)(chain['expirationDate'])}, ignore_index=True )

    for date in optionsMapPut:
        for strike in optionsMapPut[date]:
            chain = optionsMapPut[date][strike][0]
            optionsInfoPut = optionsInfoPut.append( { "Strike": (float)(strike),
                            "Price": (chain['last']),
                            "Contract Name": chain['symbol'],
                            'Volume' : chain['totalVolume'],
                            'Type' : chain['putCall'],
                            'Expiration Date' : (int)(chain['expirationDate'])}, ignore_index=True )
    optionsInfoCall = optionsInfoCall[optionsInfoCall['Volume'] != 0]
    optionsInfoPut = optionsInfoPut[optionsInfoPut['Volume'] != 0]
    return optionsInfoCall, optionsInfoPut

def _optionsDeleteAll(client,accountId):

    savedOrders = client.get_saved_orders_by_path(accountId).json()
    for order in savedOrders:
        client.delete_saved_order( accountId, order['savedOrderId'])

# this will create the saved orders
# tickers is a DataFrame
def optionsToBuy(tickers):

    client = get_client()
    account_selector = (int)(os.getenv('account_selector'))

    # get positions ( which returns symbols, positions data, quantity per symbol)
    position = positions()[1]

    # Get accountId
    account = position[account_selector]['securitiesAccount']['accountId']

    _optionsDeleteAll(client,account)
    for index,ticker in tickers.iterrows():
        sym = ticker['Contract Name']
        qty = ticker['Contracts To Buy']
        prc = ticker['Purchase Price']
        order_spec = tda.orders.options.option_sell_to_open_limit(sym, qty, prc)
        client.create_saved_order(account, order_spec)






# For testing
if __name__ == "__main__":
    pd.set_option("display.max_columns", None)
    pd.set_option('display.max_rows', None)
    pd.set_option('display.width', None)
    pd.set_option('display.max_colwidth', None)

    # a,j,v = positions()
    # testAccount = (int)(os.getenv('account_selector'))
    # print( json.dumps(j[testAccount]["securitiesAccount"]["positions"],indent=2))
    # for account in j:
    #     print( account['securitiesAccount']['accountId'] )
    print(optionChainForPredict('aapl'))

    # Test Option Chain
    # print(optionChain("aapl",0))
