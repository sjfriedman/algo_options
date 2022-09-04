# A simple way to get all the known symbols we track and folow

import awswrangler as wr
import boto3

def trigger_in_sqs(symbol):
    sqs = boto3.client('sqs')
    r = sqs.send_message(
        QueueUrl="https://sqs.us-east-1.amazonaws.com/794710784228/rockfinance-queue",
        MessageBody=symbol
    )

def lambda_handler(event, context):

    # Get list of tables, which is each symbol to process.
    f = wr.catalog.tables(database="options")

    # For each table(symbol) fire another lambda that does the capture.
    # This allows each one to run on its own
    client = boto3.client('sqs')
    for symbol in f['Table']:
        print(symbol)
        trigger_in_sqs(symbol)

    return {
        'statusCode': 200,
        'body': f['Table'].to_json()
    }

# For testing but if you call this with a new symbol that symbol will start tracking daily.
if __name__ == "__main__":

    # this will find a local aws profile to figure where to connect
    boto3.setup_default_session(profile_name="personal")

    # Get a list of tables in the options DB. each Table is a ticker
    f = wr.catalog.tables(database="options")
    print(f['Table'].to_json())

    # This will send the SQS message which triggers the -capture lambda.
    # trigger_in_sqs("aapl")
