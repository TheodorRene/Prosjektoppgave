import mysql.connector
from config import pagelinks_config as c

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

def get_n4j_query(x):
    cypher = "CREATE (p1:Page{id:$from_page_id, title:}) -[rel:LINKS_TO]-> (p2: Page {id:$page_id_2, title:}"





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
    for x in result:
        print(get_correct_format(x))
        print(get_n4j_query(x))


