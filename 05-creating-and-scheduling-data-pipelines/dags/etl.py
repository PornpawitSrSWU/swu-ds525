import json
import glob
import os
from typing import List

from airflow import DAG
from airflow.utils import timezone
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


def _get_files(filepath: str) -> List[str]:
    """
    Description: This function is responsible for listing the files in a directory
    """

    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root, "*.json"))
        for f in files:
            all_files.append(os.path.abspath(f))

    num_files = len(all_files)
    print(f"{num_files} files found in {filepath}")

    return all_files


def _create_tables():
    hook = PostgresHook(postgres_conn_id="my_postgres")
    conn = hook.get_conn()
    cur = conn.cursor()


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
    table_create
]
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def _process(**context):
    hook = PostgresHook(postgres_conn_id="my_postgres")
    conn = hook.get_conn()
    cur = conn.cursor()

    ti = context["ti"]

    # Get list of files from filepath
    all_files = ti.xcom_pull(task_ids="get_files", key="return_value")
    # all_files = get_files(filepath)

    for datafile in all_files:
        with open(datafile, "r") as f:
            data = json.loads(f.read())
            for each in data:
                #insert tbl_org
                if each["type"] == "IssueCommentEvent" or each["type"] == "PullRequestEvent" or each["type"] == "PullRequestReviewEvent" or each["type"] == "PullRequestReviewCommentEvent":  
                     if "org" in each:
                        insert_statement = f"""
                        INSERT INTO tbl_org (
                            org_id,
                            org_login,
                            org_url
                        ) VALUES ({each["org"]["id"]},
                                '{each["org"]["login"]}',
                                '{each["org"]["url"]}'
                            )
                        ON CONFLICT (org_id) DO NOTHING

                        """
                        cur.execute(insert_statement)
 
               
                #insert tbl_actor
                insert_statement = f"""
                    INSERT INTO tbl_actor (
                        actor_id,
                        actor_login,
                        actor_display_login,
                        actor_url
                    ) VALUES ({each["actor"]["id"]},
                            '{each["actor"]["login"]}',
                            '{each["actor"]["display_login"]}',
                            '{each["actor"]["url"]}'
                            )
                    ON CONFLICT (actor_id) DO NOTHING

                    """
                   
                cur.execute(insert_statement)

                #insert tbl_payload
                if "repo" in each:
                    insert_statement = f"""
                        INSERT INTO tbl_repo (
                                        repo_id,
                                        repo_name,
                                        repo_url
                                ) VALUES ('{each["repo"]["id"]}',
                                        '{each["repo"]["name"]}',
                                        '{each["repo"]["url"]}'
                               )
                                ON CONFLICT (repo_id) DO NOTHING
                            """
                    cur.execute(insert_statement)



                #insert tbl_event
                if each["type"] == "IssueCommentEvent" or each["type"] == "PullRequestEvent" or each["type"] == "PullRequestReviewEvent" or each["type"] == "PullRequestReviewCommentEvent":  
                    if "org" in each:
                        insert_statement = f"""
                            INSERT INTO tbl_event (
                                event_id,
                                event_type,
                                actor_id,
                                org_id,
                                repo_id,
                                event_public,
                                event_craete_at
                       
                        
                            ) VALUES ({each["id"]},
                                    '{each["type"]}',
                                    {each["actor"]["id"]},
                                    {each["org"]["id"]},
                                    {each["repo"]["id"]},
                                    '{each["public"]}',
                                    '{each["created_at"]}'
                              
                                )
                            ON CONFLICT (event_id) DO NOTHING
                            """
                        # print(insert_statement)
                        cur.execute(insert_statement)
                    
                else:
                    insert_statement = f"""
                        INSERT INTO tbl_event (
                            event_id,
                            event_type,
                            actor_id,
                            repo_id,
                            event_public,
                            event_craete_at
                       
                        
                        ) VALUES ({each["id"]},
                                '{each["type"]}',
                                 {each["actor"]["id"]},
                                 {each["repo"]["id"]},
                                 '{each["public"]}',
                                 '{each["created_at"]}'
                              
                              )
                        ON CONFLICT (event_id) DO NOTHING
                        """
                    cur.execute(insert_statement)

                conn.commit()


with DAG(
    "etl",
    start_date=timezone.datetime(2022, 10, 15),
    schedule="@daily",
    tags=["workshop"],
    catchup=False,
) as dag:

    get_files = PythonOperator(
        task_id="get_files",
        python_callable=_get_files,
        op_kwargs={
            "filepath": "/opt/airflow/dags/data",
        }
    )

    create_tables = PythonOperator(
        task_id="create_tables",
        python_callable=_create_tables,
    )
    
    process = PythonOperator(
        task_id="process",
        python_callable=_process,
    )

    # [get_files, create_tables] >> process
    get_files >> create_tables >> process