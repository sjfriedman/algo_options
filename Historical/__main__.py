

# A package/module level main can be defined,
# Allows you to run python Historical


# We are going to use command line parameters to switch symbols
# python Historical AAPL
if __name__ == "__main__":


    # Learn how to get arguments from command line.
    # Arg O will be the filename (python script)
    # Arg 1 and on will be parameters from the command line
    import sys
    if len(sys.argv) == 1:
        print( "Usage: python Historical TICKER")
        exit()

    ticker = sys.argv[1]

    import mainHistorical as historical
    testOptionableStocks = historical.testData(ticker)
    optionableStocks = historical.apply(testOptionableStocks, ticker)

    print(optionableStocks)
    print("Total Gain:", round(100*optionableStocks['Real Gains'].sum(),2))
