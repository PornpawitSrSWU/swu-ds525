# Data Modeling I

## Data Diagram
<img src="https://github.com/PornpawitSrSWU/swu-ds525/blob/main/01-data-modelling-i/data%20modeling%20i%20diagram.jpg" height="700" width="1000" >

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