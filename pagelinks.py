import mysql.connector
from neo4j import GraphDatabase
from config import pagelinks_config as c
from config import config as c2
from json import dumps
from datetime import datetime

def get_query():
    sql = """
         SELECT
           p.page_id, p.page_title,
           p2.page_id, p2.page_title
         FROM
          pagelinks pl
         INNER JOIN
          page p
            ON
              pl.pl_from=p.page_id
         INNER JOIN
           page p2
           ON
             pl.pl_title=p2.page_title
         WHERE
           pl.pl_namespace=0 AND pl.pl_from_namespace=0
          """
    if c["debug"]:
        sql += " LIMIT 1000"
    return sql

def get_query_pagelinks_complete():
    sql= "SELECT from_page_id, from_page_title, to_page_id, to_page_title FROM pagelinks_complete"
    if c["debug"]:
        sql += " LIMIT 1000"
    return sql

def get_correct_format(x):
    return f"{x[0]} {x[1].decode()} {x[2]} {x[3].decode()}"

def get_n4j_query(x, format=False):
    cypher_escaped = "MERGE (p1:Page {id:$from_page_id, title:$from_page_title}) " + \
              "MERGE (p2:Page {id:$to_page_id, title:$to_page_title}) "  + \
        "CREATE (p1) -[rel:LINKS_TO{timestamp:$timestamp}]-> (p2) "
    return cypher_escaped

from datetime import datetime

def get_kwargs(x):
    return {
        "from_page_id":x[0],
        "from_page_title":x[1].decode(),
        "to_page_id":x[2],
        "to_page_title":x[3].decode(),
        "timestamp":datetime(year=2021, month=9, day=1).isoformat()
    }

def do_query(tx, string):
    return tx.run(string)

def do_query_with_args(tx, string, args_dict):
    return tx.run(string, args_dict)

unique_constraint_query = "CREATE CONSTRAINT page_primary_key_constraint IF NOT EXISTS ON (p:Page) ASSERT (p.id) IS UNIQUE;"

if __name__=="__main__":
    db = mysql.connector.connect(
        host=c["host"],
        user=c["user"],
        password=c["password"],
        database=c["database"]
    )
    cursor = db.cursor()

    print("Config")
    print(c)
    print("Getting query")
    if c["use_pagelinks_complete"]:
        print("Getting for pagelinks_complete")
        query = get_query_pagelinks_complete()
    else:
        print("Getting for doing manual joins")
        query = get_query()
    print(datetime.now().time(), "Executing query")
    cursor.execute(query)

    print(datetime.now().time(), "Fetch all")
    result = cursor.fetchall()


    if c["dry_run"]:
        print(datetime.now().time(), "Starting dry run")
        for x in result:
            print(get_correct_format(x))
    else:
        print(datetime.now().time(), "Accessing neo4j database")
        neo4j_driver = GraphDatabase.driver(c2["uri"], auth=(c2["user"], c2["password"]))
        with neo4j_driver.session() as s:
            print(datetime.now().time(), "Making ID primary key by giving it a unique key constraint")
            s.write_transaction(lambda tx: tx.run(unique_constraint_query))
            print(datetime.now().time(), "Starting to insert data")
            for x in result:
                kwargs = get_kwargs(x)
                s.write_transaction(do_query_with_args, get_n4j_query(x), kwargs)
            print(datetime.now().time(), "Finito")


