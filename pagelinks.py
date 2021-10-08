import mysql.connector
from config import pagelinks_config as c

def get_our_format():
    sql = """
         SELECT
           p.page_id, p.page_title
           p2.page_id, p2.page_title
         FROM
          pagelink pl
         INNER JOIN
          page p
            ON
              pl.pl_from=p.page_id
         INNER JOIN
           page p2
           ON
             pl.pl_title=p2.page_title
         WHERE
           pl.namespace=0 AND pl.from_namespace=0
          """
    if c.debug:
        sql += " LIMIT 100"
    return sql





if __init__="__main__":
    db = mysql.connector.connect(
        host=c.host,
        user=c.user,
        password=c.password
    )
    cursor = db.cursor()
    query = get_our_format()
    cursor.execute(query)
    result = cursor.fetchall()
    for x in result:
        print(x)
    cursor.commit()


