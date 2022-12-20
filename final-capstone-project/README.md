# Project capstone By Pornpawit Saraboon


## Data modelling: Data lake
<img src="https://github.com/PornpawitSrSWU/swu-ds525/blob/main/final-capstone-project/Pic/datalake.png" height="500" width="250" center >

## Data modelling: Data warehouse
<img src="https://github.com/PornpawitSrSWU/swu-ds525/blob/main/final-capstone-project/Pic/datalake.png" height="750" width="500" >

## Getting Started

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