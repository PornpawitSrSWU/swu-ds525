import psycopg2

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
s3.meta.client.upload_file("Pizza_.csv", 'pizzasaleproject', 'Pizza_.csv')

drop_table_queries = [ """
    DROP TABLE IF EXISTS pizzasale;
    """
]
create_table_queries = [
    
    """
    CREATE TABLE IF NOT EXISTS pizzasale (
        order_detail_id int,
        order_id int,
        pizza_id text,
        quantity int,
        order_date date,
        order_time time,
        unit_price float,
        total_price float,
        pizza_size text,
        pizza_category text,
        pizza_ingredients text,
        pizza_name text
        
    );
    """

]
copy_table_queries = [
    """
    COPY pizzasale FROM 's3://pizzasaleproject/Pizza Sales.csv'
    ACCESS_KEY_ID 'ASIAW5OU4XNTULXUTEC5'
    SECRET_ACCESS_KEY '2McteWugfy8xsoS5YDcHkF2WsguaJlLrGFlhok7s'
    SESSION_TOKEN 'FwoGZXIvYXdzEDAaDGPSuXGkYBuEl3bZsyLKAYsWfrFl5udebt56V3uKS3yFqKSQDNpNP07BvJ57xgqYlGw6W5yFcss1+PXlRYqcXZkWa5VUzIQVsxjUY5U0Mp9RZ6Idx+kSZvBkQNBY3F1w4sZd6CNXiDbIlI0rnYNGP50Zu/wg1XWe59rqW4PllYsgbYVYOTIemTKwjjcgwyEjt+Vs5TY1b1grNb9YHpJK/tGJE0Y95Y6yn3S1qgljyGkwBphdTC/KenOKuyUajjPr63ggSdyup8X+ATkIAqoR67frKTfntXIrrkUo0JKAnQYyLUqbAvBnWZyoCGOG+wlGDU9fDqZl3lY5dLKHkBDo0wrYR3SrxpnabZaxquSB4w=='
    CSV
    IGNOREHEADER 1
    REGION 'us-east-1'
    """
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
    host = "pizzasaleclus.crjjtklftimj.us-east-1.redshift.amazonaws.com"
    dbname = "dev"
    user = "awsuser"
    password = "Wer121137"
    port = "5439"
    conn_str = f"host={host} dbname={dbname} user={user} password={password} port={port}"
    conn = psycopg2.connect(conn_str)
    cur = conn.cursor()

    drop_table_query = "DROP TABLE IF EXISTS pizzasale"
    cur.execute(drop_table_query)
    conn.commit()

    create_table_queries = """
    CREATE TABLE IF NOT EXISTS pizzasale (
        order_detail_id int,
        order_id int,
        pizza_id text,
        quantity int,
        order_date DATE,
        order_time TIME,
        unit_price float,
        total_price float,
        pizza_size text,
        pizza_category text,
        pizza_ingredients text,
        pizza_name text
        
    )
    """
    cur.execute(create_table_queries)
    conn.commit()

    copy_table_queries = """
    COPY pizzasale FROM 's3://pizzasaleproject/Pizza_.csv'
    ACCESS_KEY_ID 'ASIAW5OU4XNTULXUTEC5'
    SECRET_ACCESS_KEY '2McteWugfy8xsoS5YDcHkF2WsguaJlLrGFlhok7s'
    SESSION_TOKEN 'FwoGZXIvYXdzEDAaDGPSuXGkYBuEl3bZsyLKAYsWfrFl5udebt56V3uKS3yFqKSQDNpNP07BvJ57xgqYlGw6W5yFcss1+PXlRYqcXZkWa5VUzIQVsxjUY5U0Mp9RZ6Idx+kSZvBkQNBY3F1w4sZd6CNXiDbIlI0rnYNGP50Zu/wg1XWe59rqW4PllYsgbYVYOTIemTKwjjcgwyEjt+Vs5TY1b1grNb9YHpJK/tGJE0Y95Y6yn3S1qgljyGkwBphdTC/KenOKuyUajjPr63ggSdyup8X+ATkIAqoR67frKTfntXIrrkUo0JKAnQYyLUqbAvBnWZyoCGOG+wlGDU9fDqZl3lY5dLKHkBDo0wrYR3SrxpnabZaxquSB4w=='
    CSV
    IGNOREHEADER 1
    REGION 'us-east-1'
    """
    cur.execute(copy_table_queries)
    conn.commit()


    #drop_tables(cur, conn)
    #create_tables(cur, conn)
    #load_staging_tables(cur, conn)

    #insert_tables(cur, conn)

    #query = "select * from events"
    #cur.execute(query)
    #records = cur.fetchall()
    #for row in records:
    #    print(row)

    conn.close()


if __name__ == "__main__":
    main()