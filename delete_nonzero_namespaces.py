from config import config as c
from neo4j import GraphDatabase
from sys import argv
from datetime import datetime


def do_stuff(tx,page_id):
    query = "MATCH (p:Page{id:$page_id}) DETACH DELETE p"
    print(tx.run(query,page_id=page_id).consume().counters)

if __name__ == "__main__":
    print("Initalizing database connection", datetime.now())
    driver = GraphDatabase.driver(c["uri"], auth=(c["user"], c["password"]))
    print("Opening feil")
    with open(argv[1]) as f:
        print("Opening session")
        with driver.session() as s:
            first_line=True
            for line in f:
                if first_line:
                    first_line=False
                else:
                    s.write_transaction(do_stuff, int(line))
    print("Finished", datetime.now())



