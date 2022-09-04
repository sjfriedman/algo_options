import os
import pickle
from datetime import date
from dotenv import load_dotenv

# For check that directory exists for cache
if not os.path.isdir(".symbol_cache"):
    os.mkdir(".symbol_cache")

def _get_path(ticker):
    path = './.symbol_cache/'+ticker+'/'
    if not os.path.isdir(path):
        os.mkdir(path)
    return path

def _get_file(ticker, key):
    ticker = ticker.upper()
    path = _get_path(ticker)
    today = date.today().isoformat()
    file = path + key + '.' + ticker + '.' + today + '.pickle'
    return file

def delete( ticker, key ):
    try:
        os.remove( _get_file(ticker,key) )
    except:
        pass

def get(ticker,key):

    load_dotenv()
    dont_cache = os.getenv("dont_cache", "")
    if key in dont_cache:
        return None

    value = None
    try:
        with open( _get_file(ticker,key), 'rb') as f:
            value = pickle.load(f)
            # if value['updated'] != date.today():
            #     value = None
            # else:
            #     value = value['value']
    except Exception as e:
        pass
    if value is False:
        raise Exception('Data stored false')
    return value

def set(ticker,key,value):
    load_dotenv()
    dont_cache = os.getenv("dont_cache", "")
    if key in dont_cache:
        return

    try:
        # data = { 'updated': date.today(), 'value': value }
        with open( _get_file(ticker,key), 'wb') as f:
            pickle.dump(value,f, pickle.HIGHEST_PROTOCOL)
    except Exception as e:
        print("oops", e)
        pass

# For testing
if __name__ == "__main__":
    price = get("ABNB","live_price")
    print( "ABNB", price )
    earnings = get("ABNB","y.earnings")
    print( "ABNB", price )
