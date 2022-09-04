from dotenv import load_dotenv
import os

# Base directory where we get stock symbol data and store caches.
def getBaseDir(ticker):
    # Get path to options data for environment.
    load_dotenv()
    basedir = os.getenv("options_basedir", None)
    if not basedir:
        basedir = os.getcwd() + "/Data/"
    elif not basedir.endswith("/"):
        basedir += "/"

    path = basedir + ticker.upper() + "/"
    if not os.path.isdir(path):
        os.mkdir(path)

    return path
