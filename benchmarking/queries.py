from utils import parse_multiple_id_regex

bucket = "pageviews_b_namespace_eq_0"
"""
How many people visited P_i in total?
"""
Q1_neo = "" +\
        ("MATCH (page:Page{id:$page_id}) "
         "MATCH (page)-[:FIRST]->(p1:PageView)-[:NEXT*0..]->(p3:PageView) "
         "WITH sum (p3.count) as total "
         "RETURN total ")

Q1_influx = "" + \
    (f'from(bucket: "{bucket}")'
       '|> range(start: timestart, stop: timestop)'
       '|> filter(fn: (r) => r["_measurement"] == "pageview")'
       '|> filter(fn: (r) => r["_field"] == "hits")'
       '|> filter(fn: (r) => r["page_id"] == page_id)'
       '|> sum(column: "_value")')


"""
Q2: How many people visited P_i in T_i
"""
def Q2_neo(from_time, to_time):
    """
    :param from_time: The start of the interval, in ISO date format.
    :param to_time: The end of the interval, in ISO date format.
    """
    query = "" + \
            ("MATCH (page:Page{id:$page_id}) "
            "CALL { "
            "WITH page "
            "MATCH (page)-[:FIRST]->(p1:PageView)-[:NEXT*0..]->(p2:PageView)-[:NEXT*0..]->(p3:PageView) "
            f"WHERE p2.timestamp >= datetime('{from_time}') AND p3.timestamp <= datetime('{to_time}') "
            "WITH p2, sum (p2.count) as total "
            "RETURN total "
            "LIMIT 1 "
            "} "

            "RETURN page, total "
            "LIMIT 1;")
    return query

# number_of_hits_for_a_range_page_id
Q2_influx = "" + \
    (f'from(bucket: "{bucket}")'
       '|> range(start: timestart, stop: timestop)'
       '|> filter(fn: (r) => r["_measurement"] == "pageview")'
       '|> filter(fn: (r) => r["_field"] == "hits")'
       '|> filter(fn: (r) => r["page_id"] == page_id)'
       '|> sum(column: "_value")')

"""
Q3
"""
# get_page_with_highest_hits_in_a_range
Q3_influx= "" + \
    (f'from(bucket: "{bucket}")'
       '|> range(start: timestart, stop: timestop)'
       '|> filter(fn: (r) => r["_measurement"] == "pageview")'
       '|> filter(fn: (r) => r["_field"] == "hits")'
       '|> sum(column: "_value")'
       '|> mean(column: "_value")'
       '|> highestMax(n: 1, groupColumns: ["_value"])')

Q3_neo = "Too slow"

"""
Q4
"""

#def get_average_number_of_hits_for_a_range_multiple_page_ids(page_ids):
Q4_influx = "" + \
    (f'from(bucket: "{bucket}")'
       '|> range(start: timestart, stop: timestop)'
       '|> filter(fn: (r) => r["_measurement"] == "pageview")'
       '|> filter(fn: (r) => r["_field"] == "hits")'
       '|> sum(column: "_value")'
       '|> group(columns: ["_measurement"], mode:"by")'
       '|> mean(column: "_value")')

Q4_neo = "Too slow"


"""
Q6
"""
Q6_influx = Q2_influx

Q6_get_timestamps = "" + \
"MATCH (p:Page{id:$page_id})-[r:LINKED_TO]->(Page) RETURN DISTINCT r.from_timestamp ORDER BY r.from_timestamp ASC;"


def Q7_get_communities(graph_name):
    """Find clusters with the Louvain community algorithm."""
    return ("" + \
        f"CALL gds.louvain.stream('{graph_name}') "
        "YIELD nodeId, communityId "
        "RETURN gds.util.asNode(nodeId).id AS id, communityId "
        "ORDER BY communityId ASC;"
    )
    return result.data()

def _Q7_create_graph_if_nonexistent(graph_name, timestamp, query_api):
    exist_result = query_api.run(f"CALL gds.graph.exists('{graph_name}') YIELD exists")
    if not exist_result.data()[0].get("exists"):
        
        query_api.run("" + \
            "CALL gds.graph.create.cypher("
                f"'{graph_name}', "
                "'MATCH (p:Page) RETURN id(p) AS id', "
                "'MATCH (p:Page)-[r:LINKED_TO]->(q:Page)"
                f""" WHERE r.from_timestamp < "{timestamp}" AND "{timestamp}" < r.to_timestamp"""
                " RETURN id(p) AS source, id(q) AS target') "
            "YIELD "
            "graphName AS graph, nodeQuery, nodeCount AS nodes, relationshipQuery, relationshipCount AS rels;")


def Q7_influx(page_ids):
    regex = parse_multiple_id_regex(page_ids)
    return "" + \
    (f'from(bucket: "{bucket}")'
       '|> range(start: timestart, stop: timestop)'
       '|> filter(fn: (r) => r["_measurement"] == "pageview")'
       '|> filter(fn: (r) => r["_field"] == "hits")'
       f'|> filter(fn: (r) => r["page_id"] =~ {regex})'
       '|> sum(column: "_value")')

def Q8_get_communities(graph_name):
    return Q7_get_communities(graph_name)

def Q8_influx(page_ids):
    regex = parse_multiple_id_regex(page_ids)
    return "" + \
    (f'from(bucket: "{bucket}")'
       '|> range(start: timestart, stop: timestop)'
       '|> filter(fn: (r) => r["_measurement"] == "pageview")'
       '|> filter(fn: (r) => r["_field"] == "hits")'
       f'|> filter(fn: (r) => r["page_id"] =~ {regex})'
       '|> mean(column: "_value")')