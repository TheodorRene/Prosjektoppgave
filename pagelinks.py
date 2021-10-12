import mysql.connector
from neo4j import GraphDatabase
from config import pagelinks_config as c
from config import config as c2
from json import dumps

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

def get_correct_format(x):
    return f"{x[0]} {x[1].decode()} {x[2]} {x[3].decode()}"

def get_n4j_query(x, format=False):
    cypher_escaped = "MERGE (p1:Page {id:$from_page_id, title:$from_page_title}) " + \
              "MERGE (p2:Page {id:$to_page_id, title:$to_page_title}) "  + \
              "CREATE (p1) -[rel:LINKS_TO]-> (p2) "
    return cypher_escaped


def get_kwargs(x):
    return {
        "from_page_id":x[0],
        "from_page_title":x[1].decode(),
        "to_page_id":x[2],
        "to_page_title":x[3].decode()
    }

def do_query(tx, string):
    return tx.run(string)

def do_query_with_args(tx, string, args_dict):
    return tx.run(string, args_dict)

if __name__=="__main__":
    db = mysql.connector.connect(
        host=c["host"],
        user=c["user"],
        password=c["password"],
        database=c["database"]
    )
    cursor = db.cursor()

    query = get_query()
    cursor.execute(query)

    result = cursor.fetchall()


    if c["dry_run"]:
        for x in result:
            print(get_correct_format(x))
    else:
        neo4j_driver = GraphDatabase.driver(c2["uri"], auth=(c2["user"], c2["password"]))
        with neo4j_driver.session() as s:
            for x in result:
                kwargs = get_kwargs(x)
                s.write_transaction(do_query_with_args, get_n4j_query(x), kwargs)


