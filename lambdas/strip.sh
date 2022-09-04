
# INCOMING FORMAT
# UnderlyingSymbol,UnderlyingPrice,Exchange,OptionSymbol,OptionExt,Type,Expiration,DataDate,Strike,Last,Bid,Ask,Volume,OpenInterest,IV,Delta,Gamma,Theta,Vega,AKA
# LUV,61.3,*,LUV210401C00030000,,call,04/01/2021,04/01/2021,30,0,30.6,32.2,0,0,0.3,1,0,0,0,LUV210401C00030000


# DESIRED FORMAT
# date, symbol, under, expiration, strike, put/call, bid, ask, price, volume, open interest, implied vol, delta, gamma, rho, theta, vega, nonstd
# 2021-01-04,LUV   210108C00035000,LUV,2021-01-08,35,C,8.55,11.8,10.55,0,2,2.06094,0.89917,0.0181695,0.00328204,-0.214463,0.00832479,104


YEAR=$1
MONTH=$2
SYMBOL=$3

CSV_FILE="$SYMBOL-$YEAR-$MONTH"

echo "under,_up,_e,symbol,_oe,put/call,expiration,date,strike,price,bid,ask,volume,open interest,implied vol,Delta,Gamma,Theta,Vega,AKA" > "$CSV_FILE.tmp"
cat *_options_*.csv | grep -i "^$SYMBOL," | sed "s/call/C/g" | sed "s/put/P/g" >> "$CSV_FILE.tmp"

# mv "$CSV_FILE.tmp" "$CSV_FILE.csv"


# python ~/github.com/rfriedman/samfin/lambdas/lambda_daily.py | jq -j '.[] | tostring + "\u0000"' | xargs -0 -n1 ~/github.com/rfriedman/samfin/lambdas/strip.sh 2021 04
# for f in *.tmp; do mv $f  ${f%.tmp}.csv; done
