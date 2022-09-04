# Deploy lambda to AWS
cd lambdas

zip historical.zip lambda_historical.py
aws --profile personal lambda update-function-code --function-name rockfinance-historical --zip-file fileb://historical.zip
rm historical.zip

zip capture.zip lambda_capture.py
aws --profile personal lambda update-function-code --function-name rockfinance-capture --zip-file fileb://capture.zip
rm capture.zip

zip daily.zip lambda_daily.py
aws --profile personal lambda update-function-code --function-name rockfinance-daily --zip-file fileb://daily.zip
rm daily.zip

zip checker.zip lambda_checker.py
aws --profile personal lambda update-function-code --function-name rockfinance-checker --zip-file fileb://checker.zip
rm checker.zip
