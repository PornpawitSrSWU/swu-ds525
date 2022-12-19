import boto3


aws_access_key_id = "ASIAW5OU4XNTULXUTEC5"
aws_secret_access_key = "2McteWugfy8xsoS5YDcHkF2WsguaJlLrGFlhok7s"
aws_session_token = "FwoGZXIvYXdzEDAaDGPSuXGkYBuEl3bZsyLKAYsWfrFl5udebt56V3uKS3yFqKSQDNpNP07BvJ57xgqYlGw6W5yFcss1+PXlRYqcXZkWa5VUzIQVsxjUY5U0Mp9RZ6Idx+kSZvBkQNBY3F1w4sZd6CNXiDbIlI0rnYNGP50Zu/wg1XWe59rqW4PllYsgbYVYOTIemTKwjjcgwyEjt+Vs5TY1b1grNb9YHpJK/tGJE0Y95Y6yn3S1qgljyGkwBphdTC/KenOKuyUajjPr63ggSdyup8X+ATkIAqoR67frKTfntXIrrkUo0JKAnQYyLUqbAvBnWZyoCGOG+wlGDU9fDqZl3lY5dLKHkBDo0wrYR3SrxpnabZaxquSB4w=="


s3 = boto3.resource(
    "s3",
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key,
    aws_session_token=aws_session_token

)
s3.meta.client.upload_file("Pizza Sales.xlsx", 'pizzasaleproject', 'Pizza Sales.xlsx')