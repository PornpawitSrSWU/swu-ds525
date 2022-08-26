import psycopg2


table_drop = """
    DROP TABLE IF EXISTS tbl_author;
    DROP TABLE IF EXISTS tbl_issue;
    DROP TABLE IF EXISTS tbl_org;
    DROP TABLE IF EXISTS tbl_author;
    DROP TABLE IF EXISTS tbl_commit;
    DROP TABLE IF EXISTS tbl_payload;
    DROP TABLE IF EXISTS tbl_actor;
    DROP TABLE IF EXISTS tbl_event;

"""

table_create = """
    CREATE TABLE IF NOT EXISTS tbl_author (
        author_id INTEGER NOT NULL,
        author_name VARCHAR(250),
        author_email VARCHAR(250),
        CONSTRAINT tbl_author_PK PRIMARY KEY(author_id)
    );

    CREATE TABLE IF NOT EXISTS tbl_issue (
        issue_id INTEGER NOT NULL,
        issue_url VARCHAR(250),
        issue_title VARCHAR(250),
        issue_number INTEGER,
        actor_id INTEGER,
        CONSTRAINT tbl_issue_PK PRIMARY KEY(issue_id)
    );

    CREATE TABLE IF NOT EXISTS tbl_org (
        org_id INTEGER NOT NULL,
        org_login VARCHAR(250),
        org_gravatar_id VARCHAR(250),
        org_url VARCHAR(250),
        CONSTRAINT tbl_org_PK PRIMARY KEY(org_id)
    );


    CREATE TABLE IF NOT EXISTS tbl_commit (
        commit_id INTEGER NOT NULL,
        commit_sha VARCHAR(250),
        author_id INTEGER,
        commit_message VARCHAR(250),
        commit_url VARCHAR(250),
        CONSTRAINT tbl_commit_PK PRIMARY KEY(commit_id),
        CONSTRAINT tbl_commit_FK1 FOREIGN KEY(author_id) REFERENCES tbl_author(author_id)
    );

    CREATE TABLE IF NOT EXISTS tbl_payload (
        payload_id INTEGER NOT NULL,
        payload_action VARCHAR(250),
        issue_id INTEGER,
        commit_id INTEGER,
        CONSTRAINT tbl_payload_PK PRIMARY KEY(payload_id),
        CONSTRAINT tbl_payload_FK1 FOREIGN KEY(issue_id) REFERENCES tbl_issue(issue_id),
        CONSTRAINT tbl_payload_FK2 FOREIGN KEY(commit_id) REFERENCES tbl_commit(commit_id)
    );

    CREATE TABLE IF NOT EXISTS tbl_actor (
        actor_id INTEGER NOT NULL,
        actor_login VARCHAR(250),
        actor_display_login VARCHAR(250),
        actor_url VARCHAR(250),
        actor_node_id VARCHAR(250),
        CONSTRAINT tbl_actor_PK PRIMARY KEY(actor_id)
    );

    CREATE TABLE IF NOT EXISTS tbl_event (
        event_id INTEGER NOT NULL,
        event_type VARCHAR(250),
        actor_id INTEGER,
        payload_id INTEGER,
        event_public BOOLEAN,
        event_craete_at TIMESTAMP,
        org_id INTEGER,
        CONSTRAINT tbl_event_PK PRIMARY KEY(event_id),
        CONSTRAINT tbl_event_FK1 FOREIGN KEY(actor_id) REFERENCES tbl_actor(actor_id),
        CONSTRAINT tbl_event_FK2 FOREIGN KEY(payload_id) REFERENCES tbl_payload(payload_id),
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