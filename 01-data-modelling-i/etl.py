import glob
import json
import os
from typing import List

import psycopg2


table_insert = """
    INSERT INTO users (
        xxx
    ) VALUES (%s)
    ON CONFLICT (xxx) DO NOTHING
"""


def get_files(filepath: str) -> List[str]:
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


def process(cur, conn, filepath):
    # Get list of files from filepath
    all_files = get_files(filepath)

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


def main():
    conn = psycopg2.connect(
        "host=127.0.0.1 dbname=postgres user=postgres password=postgres"
    )
    cur = conn.cursor()

    process(cur, conn, filepath="../data")

    conn.close()


if __name__ == "__main__":
    main()