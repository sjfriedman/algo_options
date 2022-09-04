from yahoo_fin import stock_info as si
import pandas as pd
import day_cache as cache
# for test
import td

# Wraps yahoo finance calls in local daily cache
def value(ticker, key = None ):

    quote_table = cache.get(ticker,'quote_table')
    if quote_table is None:
        quote_table = si.get_quote_table(ticker)
        cache.set(ticker,'quote_table',quote_table)
    # print(quote_table)

    # cache.delete(ticker,'stats')
    stats = cache.get(ticker,'stats')
    if stats is None:
        stats = si.get_stats(ticker)
        cache.set(ticker,'stats',stats)
    stats.rename(columns = {'Value':ticker}, inplace = True)
    stats.set_index('Attribute', inplace = True)
    # print(stats.to_csv())

    try:
        # cache.delete(ticker,'stats_valuation')
        stats_valuation = cache.get(ticker,'stats_valuation')
        if stats_valuation is None:
            stats_valuation = si.get_stats_valuation(ticker)
            cache.set(ticker,'stats_valuation',stats_valuation)
        # print(stats_valuation)
    except:
        stats_valuation = None
        cache.set(ticker,'stats_valuation',False)

    # gets and array of data tables
    # Earnings Estimates, Revenue Estimate, Earhings History, EPS Trend, EPS REvisions, Growth Estimates
    analysts = cache.get(ticker,'analysts_info')
    if analysts is None:
        analysts = si.get_analysts_info(ticker)
        cache.set(ticker,'analysts_info',analysts)
    # print(analysts)

    # Balance Sheet is a set of fields and columns by date recorded
    try:
        balance_sheet = cache.get(ticker,'balance_sheet')
        if balance_sheet is None :
            balance_sheet = si.get_balance_sheet(ticker)
            cache.set(ticker,'balance_sheet',balance_sheet)
        # print( balance_sheet )
    except:
        balance_sheet = None
        cache.set(ticker,'balance_sheet',False)

    # Cash Flow is a set of fields and columns by date recorded
    try:
        cash_flow = cache.get(ticker,'cash_flow')
        if cash_flow is None:
            cash_flow = si.get_cash_flow(ticker)
            cache.set(ticker,'cash_flow',cash_flow)
        # print( cash_flow )
    except :
        cash_flow = None
        cache.set(ticker,'cash_flow',False)

    # Income statement by
    try:
        income_statement = cache.get(ticker,'income_statement')
        if income_statement is None:
            income_statement = si.get_income_statement(ticker)
            cache.set(ticker,'income_statement',income_statement)
        # print( income_statement )
    except :
        income_statement = None
        cache.set(ticker,'income_statement',False)

    # Earnings data
    try:
        earnings = cache.get(ticker,'earnings')
        if earnings is None:
            earnings = si.get_earnings(ticker)
            cache.set(ticker,'earnings',earnings)
        # print( earnings )
    except :
        earnings = None
        cache.set(ticker,'earnings',False)

    # Company holders
    holders = cache.get(ticker,'holders')
    if holders is None:
        holders = si.get_holders(ticker)
        cache.set(ticker,'holders',holders)
    # print( holders )

    # Financials data over time.
    # yearly_income_statement, yearly_balance_sheet, yearly_cash_flow,
    # quarterly_income_statement, quarterly_balance_sheet, quarterly_cash_flow,
    try:
        financials = cache.get(ticker,'financials')
        if financials is None:
            financials = si.get_financials(ticker, True, True)
            cache.set(ticker,'financials',financials)
    except :
        financials = None
        cache.set(ticker,'financials',False)
    # for index in financials:
    #     print(index)
    # print( financials )

    dict = {
        'quote_table': quote_table,
        'stats':stats,
        'stats_valuation':stats_valuation,
        'analysts':analysts,
        'balance_sheet':balance_sheet,
        'cash_flow':cash_flow,
        'income_statement':income_statement,
        'earnings':earnings,
        'holders':holders,
        'financials':financials
    }

    if key:
        return dict[key]
    else:
        return dict


# For testing
if __name__ == "__main__":
    positions = td.positions()[0]
    # print(positions)
    df = pd.DataFrame()
    for p in positions:
        # print(p)
        try:
            s = value(p,"stats")
            df = pd.concat([df,s], axis=1)
        except Exception as e :
            print("Fail to load", e)
            # break

    # print(df)
    # df = value("adp","stats")
    # print(df)
    # sy = value("ibm", "stats")
    # print(sy)
    # df = df.merge(sy,left_on="Attribute",right_on="Attribute")
    # # df.insert(loc=len(df.columns), column='ibm', value= sy['ibm'])
    print(df.to_csv())
