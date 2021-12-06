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
       '|> filter(fn: (r) => r["_measurement"] == "pageview")'
       '|> filter(fn: (r) => r["_field"] == "hits")'
       '|> filter(fn: (r) => r["page_id"] == page_id)'
       '|> sum(column: "_value")')


"""
Q2: How many people visited P_i in T_i
"""
def Q2_neo(from_time, to_time):
    """
    Get pageviews for pages in a given range.
    :param from_time: The start of the interval, in ISO date format.
    :param to_time: The end of the interval, in ISO date format.
    """
    query = "" + \
        ("MATCH (page:Page{id:$page_id}) "
         "MATCH (page)-[:FIRST]->(p1:PageView)-[:NEXT*0..]->(p2:PageView)-[:NEXT*0..]->(p3:PageView) "
         f"WHERE p2.timestamp >= datetime({from_time}) AND p3.timestamp <= datetime({to_time}) "
         "WITH sum (p3.count) as total "
         "RETURN total ")
    
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
