import psycopg2


table_drop = """
    DROP TABLE IF EXISTS tbl_event;
    DROP TABLE IF EXISTS tbl_actor;
    DROP TABLE IF EXISTS tbl_repo;
    DROP TABLE IF EXISTS tbl_org;

  

    

"""

table_create = """
  

    CREATE TABLE IF NOT EXISTS tbl_org (
        org_id VARCHAR(250) NOT NULL,
        org_login VARCHAR(250),
        org_url VARCHAR(250),
        CONSTRAINT tbl_org_PK PRIMARY KEY(org_id)
    );


    CREATE TABLE IF NOT EXISTS tbl_repo (
        repo_id VARCHAR(250) NOT NULL,
        repo_name VARCHAR(250),
        repo_url VARCHAR(250),
        CONSTRAINT tbl_repo_PK PRIMARY KEY(repo_id)
    );

    CREATE TABLE IF NOT EXISTS tbl_actor (
        actor_id INTEGER NOT NULL,
        actor_login VARCHAR(250),
        actor_display_login VARCHAR(250),
        actor_url VARCHAR(250),
        CONSTRAINT tbl_actor_PK PRIMARY KEY(actor_id)
    );

    CREATE TABLE IF NOT EXISTS tbl_event (
        event_id BIGINT NOT NULL,
        event_type VARCHAR(250),
        actor_id INTEGER,
        repo_id VARCHAR(250),
        event_public BOOLEAN,
        event_craete_at TIMESTAMP,
        org_id VARCHAR(250),
        CONSTRAINT tbl_event_PK PRIMARY KEY(event_id),
        CONSTRAINT tbl_event_FK1 FOREIGN KEY(actor_id) REFERENCES tbl_actor(actor_id),
        CONSTRAINT tbl_event_FK2 FOREIGN KEY(repo_id) REFERENCES tbl_repo(repo_id),
        CONSTRAINT tbl_event_FK3 FOREIGN KEY(org_id) REFERENCES tbl_org(org_id)
    );
"""

create_table_queries = [
    table_create,
]
drop_table_queries = [
    table_drop,
]


def drop_tables(cur, conn) -> None:
    """
    Drops each table using the queries in `drop_table_queries` list.
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn) -> None:
    """
    Creates each table using the queries in `create_table_queries` list.
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    - Drops (if exists) and Creates the sparkify database.
    - Establishes connection with the sparkify database and gets
    cursor to it.
    - Drops all the tables.
    - Creates all tables needed.
    - Finally, closes the connection.
    """
    conn = psycopg2.connect(
        "host=127.0.0.1 dbname=postgres user=postgres password=postgres"
    )
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()