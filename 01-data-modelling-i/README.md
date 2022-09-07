# Data Modeling I

## Data Diagram
![alt text](https://drive.google.com/file/d/1jvvqc0eaHzsRfq44NreOxAW-Qt19jWv0/view?usp=sharing)

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