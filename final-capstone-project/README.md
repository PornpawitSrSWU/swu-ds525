# Project capstone By Pornpawit Saraboon


## Data modelling: Data lake
<img src="https://github.com/PornpawitSrSWU/swu-ds525/blob/main/final-capstone-project/Pic/datalake.png" height="500" width="250" center >

## Data modelling: Data warehouse
<img src="https://github.com/PornpawitSrSWU/swu-ds525/blob/main/final-capstone-project/Pic/datawarehouse.png" height="700" width="1050" >

## Project Documentation
[Documentation link](https://github.com/PornpawitSrSWU/swu-ds525/blob/main/final-capstone-project/Doc/Document.pdf)

## Project Presentation
[Presentation link](https://github.com/PornpawitSrSWU/swu-ds525/blob/main/final-capstone-project/Doc/Presentation.pdf)
<br>
<img src="https://github.com/PornpawitSrSWU/swu-ds525/blob/main/final-capstone-project/Pic/Presentation.png" height="700" width="1200" >

## Project implement

### 1. Prepare your AWS access
GET your credential on AWS terminal
```sh
$ cat ~/.aws/credentials
```
<br>
<img src="https://github.com/PornpawitSrSWU/swu-ds525/blob/main/final-capstone-project/Pic/credential.png" height="150" width="900" center >
<br>
- aws_access_key_id <br>
- aws_secret_access_key <br>
- aws_session_token <br>

AWS credential is used in etl.py file
<br>

```sh
def _upload_files():
    aws_access_key_id = "your aws_access_key_id"
    aws_secret_access_key = "your aws_secret_access_key"
    aws_session_token = "your aws_session_token"
```
```sh
def _insert_data():
    copy_table_queries = """
    COPY pizzasale FROM 's3://Your bucket/output.csv'
    ACCESS_KEY_ID 'your aws_access_key_id'
    SECRET_ACCESS_KEY 'your aws_secret_access_key'
    SESSION_TOKEN 'your aws_session_token'
    CSV
    IGNOREHEADER 1
    REGION 'us-east-1'
    """
    cur.execute(copy_table_queries)
    conn.commit()
```

### 2. Create data lake (S3)
Create for Your RAW data <br>
and uncheck "Block all public access"  <br>

### 3. Create Data warehouse (Redshift)
Create for Your clearned data <br>
change "Edit publicly accessible  Block all public access" to check  <br>
and Redshift cluster is used in etl file <br>
```sh
host = "pizzasaleclus.crjjtklftimj.us-east-1.redshift.amazonaws.com"
dbname = "dev"
user = "awsuser"
password = "Wer121137"
port = "5439"
conn_str = f"host={host} dbname={dbname} user={user} password={password} port={port}"
conn = psycopg2.connect(conn_str)
cur = conn.cursor()
```
### 4. Change directory
```sh
cd final-capstone-project
```
### 5.Create virtual environment "ENV"
```sh
$ python -m venv ENV
```

### 6. Activate the visual environment
```sh
$ source ENV/bin/activate
```
### 7. Install libraries from requirement.txt
```sh
$ pip install -r requirements.txt
```
### 8. Prepare environment workspace thru Docker:

If Linux system, run following commands (for Airflow usage)<br>

```sh
mkdir -p ./dags ./logs ./plugins
```
```sh
echo -e "AIRFLOW_UID=$(id -u)" > .env
```
<br> After that, run below command to start Docker <br>

```sh
docker-compose up
```
### 9. Set connection in Airflow
Access Airflow UI by port 8080 (localhost:8080) with below credential<br>
- Username: "airflow"<br>
- Password: "airflow"<br>
click on Connection menu and set following this picture <br>
<img src="https://github.com/PornpawitSrSWU/swu-ds525/blob/main/final-capstone-project/Pic/airflow.png" height="700" width="1050">



## Running Postgres

```sh
docker-compose up
```

To shutdown, press Ctrl+C and run:

```sh
docker-compose down
```
## Create Table

Run create_tables.py file to create tables

```sh
python create_tables.py
```
## Insert data from data folder to tables

Run etl.py file to insert data

```sh
python etl.py
```