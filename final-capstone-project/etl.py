import psycopg2
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession

def sparks3toredshift():
       #configure pyspark environment
    myconf = (SparkConf()\
        .setMaster("spark://<master_ip_instance>:7077")\
        .setAppName("pysparks3toredshift")\
        .set("spark.executor.memory","2g"))

    myconf.set("spark.driver.memory","1g")
    #set the jar packages to include on the Spark driver and executor classpaths
    myconf.set("spark.jars.packages","com.amazon.redshift:redshift-jdbc42-no-awssdk:1.2.45.1069,com.amazonaws:aws-java-sdk:1.7.4,org.apache.hadoop:hadoop-auth:2.7.4,org.apache.hadoop:hadoop-common:2.7.4,com.google.code.findbugs:jsr305:3.0.2,asm:asm:3.2,org.slf4j:slf4j-api:1.7.30,org.xerial.snappy:snappy-java:1.1.7.5,org.slf4j:slf4j-log4j12:1.7.30,org.apache.hadoop:hadoop-aws:2.7.3")
    myconf.set("spark.executor.extraJavaOptions","-Dcom.amazonaws.services.s3.enableV4=true")
    myconf.set("spark.driver.extraJavaOptions","-Dcom.amazonaws.services.s3.enableV4=true")

    #set up SparkContext
    sc = SparkContext(conf = myconf)
    sc.setSystemProperty("com.amazonaws.services.s3.enableV4", "true")
    
    #Redshift JDBC driver requires hadoop configuration (S3a method)
    hadoopConf = sc._jsc.hadoopConfiguration()
    hadoopConf.set("fs.s3a.access.key", "xxx")
    hadoopConf.set("fs.s3a.secret.key", "xxxxx")
    hadoopConf.set("fs.s3a.endpoint", "xxxxxx.amazonaws.com")
    hadoopConf.set("com.amazonaws.services.s3.enableV4", "true")
    hadoopConf.set("fs.s3a.impl","org.apache.hadoop.fs.s3a.S3AFileSystem")

    #set up SparkSession
    spark_session = SparkSession(sc)
    
    #read s3 data as dataframe
    df = spark_session.read.option("header",True).csv("s3a://mybucket-test2/users_info/user_info.csv")

    df.createOrReplaceTempView("dftemp")

    df_join = spark_session.sql("SELECT A.client_id, A.username, A.city, A.country, A.phone, A.sales, A.purchase, (B.price * A.purchase) AS cost, B.currency FROM df5temp A LEFT JOIN df4temp B ON A.country=B.country")
    
    #write data to redshift
    df_join.write \
        .format("jdbc") \
        .option("url","jdbc:redshift://xxxxxx.redshift.amazonaws.com:5439/mydatabase") \
        .option("dbtable","myredshiftschema.myredshifttable00") \
        .option("UID","xxxx") \
        .option("PWD","xxxxx") \
        .option("driver","com.amazon.redshift.jdbc42.Driver") \
        .mode("append") \
        .save()



drop_table_queries = [ """
    DROP TABLE IF EXISTS top10pizzabyyear;
    DROP TABLE IF EXISTS top10pizzabymonth;
    DROP TABLE IF EXISTS top10pizzabybesttimesale;
    DROP TABLE IF EXISTS 10badpizza;
    """
]
create_table_queries = [
    """
    CREATE TABLE IF NOT EXISTS events_top
    (
        type text,
        number_of_events int,
        PRIMARY KEY (
            type,number_of_events
        )
    );
    """
    ,
    """
    CREATE TABLE IF NOT EXISTS top10pizzabyyear (
        pizza_name text,
        number_of_sales int,
        year int,
        PRIMARY KEY (
            pizza_name, year, number_of_sales
        )
    );
    """,
    
    """
    CREATE TABLE IF NOT EXISTS top10pizzabymonth (
        pizza_name text,
        number_of_sales int,
        month int,
        PRIMARY KEY (
            pizza_name, month, number_of_sales
        )
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS top10pizzabybesttimesale (
        pizza_name text,
        number_of_sales int,
        time text,
        PRIMARY KEY (
            pizza_name, time, number_of_sales
        )
    )
    """,
    """
     CREATE TABLE IF NOT EXISTS 10badpizza (
        pizza_name text,
        number_of_sales text,
        PRIMARY KEY (
            pizza_name, number_of_sales
        )
    )""",

]
copy_table_queries = [
    """
    COPY staging_events FROM 's3://pornpawitslab3/github_events_01.json'
    CREDENTIALS 'aws_iam_role=arn:aws:iam::475577236327:role/LabRole'
    JSON 's3://pornpawitslab3/events_json_path.json'
    REGION 'us-east-1'
    """,
]
insert_table_queries = [
        """
            INSERT INTO events ( id, type, actor, repo, created_at )
            SELECT DISTINCT id, type, actor_name, repo_name, created_at
            FROM staging_events
            WHERE id NOT IN (SELECT DISTINCT id FROM events)
        """,
        """
            INSERT INTO actors ( id, name, url )
            SELECT DISTINCT actor_id, actor_name, actor_url
            FROM staging_events
            WHERE actor_id NOT IN (SELECT DISTINCT id FROM actors)
        """,
        """
            INSERT INTO org ( id, login, url )
            SELECT DISTINCT org_id, org_login, org_url
            FROM staging_events
            WHERE org_id NOT IN (SELECT DISTINCT id FROM org)
        """,
]


def drop_tables(cur, conn):
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def load_staging_tables(cur, conn):
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    host = "redshift-cluster-1.crjjtklftimj.us-east-1.redshift.amazonaws.com"
    dbname = "dev"
    user = "awsuser"
    password = "Wer121137"
    port = "5439"
    conn_str = f"host={host} dbname={dbname} user={user} password={password} port={port}"
    conn = psycopg2.connect(conn_str)
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)
    load_staging_tables(cur, conn)

    insert_tables(cur, conn)

    query = "select * from events"
    cur.execute(query)
    records = cur.fetchall()
    for row in records:
        print(row)

    conn.close()


if __name__ == "__main__":
    main()