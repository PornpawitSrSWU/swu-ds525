# Project capstone By Pornpawit Saraboon


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
<img src="https://github.com/PornpawitSrSWU/swu-ds525/blob/main/final-capstone-project/Pic/airflow.png" height="500" width="1050">

### 9. Create DBT Project for create table for analytic
- Create a dbt project <br>
```sh
dbt init
```
<br>
- Edit the dbt profiles <br>

```sh
code ~/.dbt/profiles.yml
```

<br>
-Test dbt connection <br>

```sh
cd trydbredshift
```

<br>

```sh
dbt debug
```
<br>
- You should see "All checks passed!".<br>
- Create your model at trydbredshift/models<br>
- To create models 

<br>

```sh
dbt run
```

<br>
- To test models <br>

```sh
dbt test
```
<br>
- To view docs (on Gitpod)
```sh
dbt docs generate
dbt docs serve
```
<br>
<img src="https://github.com/PornpawitSrSWU/swu-ds525/blob/main/final-capstone-project/Pic/lineg.png" height="600" width="1050">


## 10. Create Dashboard by Tableau:

- Connect Tableau Desktop to Redshift by following information <br>
```sh
host = "pizzasaleclus.crjjtklftimj.us-east-1.redshift.amazonaws.com"
dbname = "dev"
user = "awsuser"
password = "Wer121137"
port = "5439"
```
<br>
<img src="https://github.com/PornpawitSrSWU/swu-ds525/blob/main/final-capstone-project/Pic/tableau_con.png" height="450" width="500">
<br>
<img src="https://github.com/PornpawitSrSWU/swu-ds525/blob/main/final-capstone-project/Pic/tableau.png" height="500" width="1050">
<br>
<img src="https://github.com/PornpawitSrSWU/swu-ds525/blob/main/final-capstone-project/Pic/tableau_dash1.png" height="500" width="1050">
<br>

[Dashboard link](https://public.tableau.com/shared/JSBGP3WZH?:display_count=n&:origin=viz_share_link)

<br>
## And finally do not forget to shutdown
-  Stop services by shutdown Docker <br>
```sh
docker-compose down
```
<br>
- Deactivate the virtual environment <br>

```sh
$ deactivate
```