# Building a Data Warehouse
## Data model
<img src="https://github.com/PornpawitSrSWU/swu-ds525/blob/main/03-building-a-data-warehouse/DataWarehouse16.jpg" height="700" width="1000" >

## Getting Started
```sh
python -m venv ENV
source ENV/bin/activate
pip install -r requirements.txt
```
## Create Redshift
<img src="https://github.com/PornpawitSrSWU/swu-ds525/blob/main/03-building-a-data-warehouse/Redshift.png" height="700" width="1000" >

## Upload JSON to S3
<img src="https://github.com/PornpawitSrSWU/swu-ds525/blob/main/03-building-a-data-warehouse/S3.png" height="700" width="1000" >

## config etl.py file to connect Redshift
    host = "redshift-cluster-1.crjjtklftimj.us-east-1.redshift.amazonaws.com"
    dbname = "dev"
    user = "awsuser"
    password = "Wer121137"
    port = "5439"

## config Config etl.py file to copy the data from S3 to Redshift
copy_table_queries = [
    """
    COPY staging_events FROM 's3://pornpawitslab3/github_events_01.json'
    CREDENTIALS 'aws_iam_role=arn:aws:iam::475577236327:role/LabRole'
    JSON 's3://pornpawitslab3/events_json_path.json'
    REGION 'us-east-1'
    """,
]

## Run etl.py file to create tables and insert data to tables

```sh
python etl.py
```