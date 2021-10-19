import os
import mysql.connector

def get_all_link_titles():
    #TODO: Remove config as it is set in #6
    c = {
    "host":"localhost",
    "user": os.environ.get("MARIADB_USER"),
    "password": os.environ.get("MARIADB_ROOT_PASSWORD"),
    "database":"pagelinks-db",
}
    db = mysql.connector.connect(
        host=c["host"],
        user=c["user"],
        password=c["password"],
        database=c["database"]
    )
    cursor = db.cursor()

    query = """SELECT DISTINCT pl_title from pagelinks"""
    cursor.execute(query)

    result = cursor.fetchall()
    return [row[0] for row in result]