from config import influx_config, config as neo_config
from datetime import datetime
from perf_time_functions import time_func_avg, time_func


from influxdb_client import InfluxDBClient
from py2neo import Graph

from queries import *

bucket="pageviews_b_namespace_eq_0"
org=influx_config["org"]
show_data=False

def debuglog(text):
    """
    Why use 5 minutes to learn pythons logging library that will help you for
    the rest of your career when you can make your own in like 10 minutes
    """
    if influx_config["debug"]:
        print(datetime.now(), text)

def getInfluxClient():
    debuglog("Connecting to influx")
    return InfluxDBClient(
        url=influx_config["url"],
        token=influx_config["token"],
        org=influx_config["org"]
        )

def getNeo4jDriver():
    return Graph(
            neo_config["uri"],
            auth=(neo_config["user"], neo_config["password"]))


# Erna_Solberg
Q1_page_id=309272

@time_func_avg
def exe_Q1_influx(query_api):
    result = query_api.query(org=org,  query=Q1_influx, params={"page_id":str(Q1_page_id),
        "timestart": datetime.fromtimestamp(0),
        "timestop": datetime.now()})
    if show_data:
        results = []
        for table in result:
            for record in table.records:
                results.append((record.values.get("page_id"), record.get_value()))
        print(results)




@time_func_avg
def exe_Q1_neo(g):
    result = g.run(Q1_neo,{"page_id":Q1_page_id})
    if show_data:
        print(result)


if __name__=="__main__":
    query_api = getInfluxClient().query_api()
    neo4j_driver = getNeo4jDriver()

    exe_Q1_influx(query_api)
    exe_Q1_neo(neo4j_driver)


