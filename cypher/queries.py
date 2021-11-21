from neo4j import GraphDatabase
import os

from dateutil import parser

config = {
    "uri":"bolt://localhost:7687",
    "user": "neo4j",
    "password":os.environ["NEO4J_PASS"],
    }

def main():
    driver = GraphDatabase.driver(config["uri"], auth=(config["user"], config["password"]))
    with driver.session() as s:
        s.write_transaction(create_louvain_communities)
        #s.write_transaction(get_views_in_time_range, from_time='2021-01-01T00:00:00', to_time='2021-01-01T12:00:00')

def get_views_in_time_range(tx, from_time, to_time):
    """
    Get pageviews for pages in a given range.
    :param from_time: The start of the interval, in ISO date format.
    :param to_time: The end of the interval, in ISO date format.
    """
    result=tx.run("" + \
        ("MATCH (page:Page) "
            "CALL { "
            "WITH page "
            "MATCH (page)-[:FIRST]->(p1:PageView)-[:NEXT*0..]->(p2:PageView)-[:NEXT*0..]->(p3:PageView) "
            f"WHERE p2.timestamp >= datetime({_iso_to_cypher_date(from_time)}) AND p3.timestamp <= datetime({_iso_to_cypher_date(to_time)}) "
            "WITH sum (p3.count) as total "
            "RETURN total "
            "LIMIT 1 "
        "} "

        "RETURN page, total "
        "LIMIT 1000;")
    )

    print(result.data())


def create_gds_graph_if_not_present(tx, graph_name="pagelinks"):
    """Greates a Neo4j GDS graph if it does not already exist."""
    exist_result = tx.run(f"CALL gds.graph.exists('{graph_name}') YIELD exists")
    if not exist_result.single().get('exists'):
        tx.run("" + \
        "CALL gds.graph.create.cypher("
            f"'{graph_name}', "
            "'MATCH (p:Page) RETURN id(p) AS id', "
            "'MATCH (p:Page)-[r:LINKS_TO]->(q:Page) RETURN id(p) AS source, id(q) AS target') "
        "YIELD "
        "graphName AS graph, nodeQuery, nodeCount AS nodes, relationshipQuery, relationshipCount AS rels;")

def create_louvain_communities(tx, graph_name="pagelinks"):
    """Find clusters with the Louvain community algorithm."""
    create_gds_graph_if_not_present(tx, graph_name)
    result = tx.run("" + \
        f"CALL gds.louvain.stream('{graph_name}') "
        "YIELD nodeId, communityId "
        "RETURN gds.util.asNode(nodeId).id AS id, communityId "
        "ORDER BY communityId, id ASC;"
    )
    print(result.data())

def _iso_to_cypher_date(iso_date):
    """Parse a ISO string to the Cyhper datetime format."""
    date = parser.parse(iso_date)
    return f"{{year:{date.year}, month:{date.month}, day:{date.day}, hour:{date.hour}, minute:{date.minute}, second:{date.second}}}"


if __name__ == "__main__":
    main()