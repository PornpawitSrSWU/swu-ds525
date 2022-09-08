# Data Modeling II

## Data Diagram
<img src="https://github.com/PornpawitSrSWU/swu-ds525/blob/main/02-data-modelling-ii/Data%20Warehouse-13.jpg" height="700" width="1000" >

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

## Running Cassandra

```sh
docker-compose up
```

To shutdown, press Ctrl+C and run:

```sh
docker-compose down
```
## Create Table, Insert Data and Select Data

Run etl.py file

```sh
python etl.py
```

## Result .png

<img src="https://github.com/PornpawitSrSWU/swu-ds525/blob/main/02-data-modelling-ii/Screenshot%20(140).png" height="700" width="1000" >
