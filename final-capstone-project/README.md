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

### 2. Change directory
```sh
cd final-capstone-project
```

```sh
python -m venv ENV
source ENV/bin/activate
pip install -r requirements.txt
```

### Prerequisite when install psycopg2 package

For Debian/Ubuntu users:

```sh
sudo apt install -y libpq-dev
```

For Mac users:

```sh
brew install postgresql
```

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