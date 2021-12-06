from neo4j import GraphDatabase
import os

from dateutil import parser

config = {
    "uri":"bolt://localhost:7687",
    "user": "neo4j",
    "password":os.environ["NEO4J_PASS"],
    }

def main():
    timestamp = "2021-09-01T13:00:0"
    graph_name = f"revisions-at-{timestamp}".replace(":", "-")
    print(graph_name)
    gds_graph_query = get_pagelinks_at_timestamp_gds_query(timestamp, graph_name)

    driver = GraphDatabase.driver(config["uri"], auth=(config["user"], config["password"]))
    with driver.session() as s:
        
        response = s.write_transaction(create_louvain_communities, gds_graph_query, graph_name)
        print(get_page_ids_by_community_id(response, 1))
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
            f"WHERE p2.timestamp >= '{from_time}' AND p3.timestamp <= '{to_time}' "
            "WITH sum (p3.count) as total "
            "RETURN total "
            "LIMIT 1 "
        "} "

        "RETURN page, total "
        "LIMIT 1000;")
    )

    print(result.data())


def create_gds_graph_if_not_present(tx, graph_query, graph_name="pagelinks"):
    """Greates a Neo4j GDS graph if it does not already exist."""
    exist_result = tx.run(f"CALL gds.graph.exists('{graph_name}') YIELD exists")
    if not exist_result.single().get('exists'):
        tx.run(graph_query)
        
def get_pagelinks_gds_query(graph_name="pagelinks"):
    return ("" + \
        "CALL gds.graph.create.cypher("
            f"'{graph_name}', "
            "'MATCH (p:Page) RETURN id(p) AS id', "
            "'MATCH (p:Page)-[r:LINKS_TO]->(q:Page) RETURN id(p) AS source, id(q) AS target') "
        "YIELD "
        "graphName AS graph, nodeQuery, nodeCount AS nodes, relationshipQuery, relationshipCount AS rels;")

def get_pagelinks_at_timestamp_gds_query(timestamp, graph_name):
    return ("" + \
        "CALL gds.graph.create.cypher("
            f"'{graph_name}', "
            "'MATCH (p:Page) RETURN id(p) AS id', "
            "'MATCH (p:Page)-[r:LINKED_TO]->(q:Page)"
            f""" WHERE r.from_timestamp < "{timestamp}" AND "{timestamp}" < r.to_timestamp"""
            " RETURN id(p) AS source, id(q) AS target') "
        "YIELD "
        "graphName AS graph, nodeQuery, nodeCount AS nodes, relationshipQuery, relationshipCount AS rels;")

def create_louvain_communities(tx, graph_query, graph_name="pagelinks"):
    """Find clusters with the Louvain community algorithm."""
    create_gds_graph_if_not_present(tx, graph_query, graph_name)
    result = tx.run("" + \
        f"CALL gds.louvain.stream('{graph_name}') "
        "YIELD nodeId, communityId "
        "RETURN gds.util.asNode(nodeId).id AS id, communityId "
        "ORDER BY id ASC;"
    )
    return result.data()

def get_page_ids_by_community_id(community_response, page_id):
    community_id = get_community_id_by_page_id(community_response, page_id)
    community_ids = [element["id"] for element in community_response if element["communityId"] == community_id]
    return community_ids

def get_community_id_by_page_id(community_response, page_id):
    return next(element["communityId"] for element in community_response if element["id"] == page_id)


if __name__ == "__main__":
    main()