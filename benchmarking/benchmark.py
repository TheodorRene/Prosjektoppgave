from datetime import datetime

from enum import Enum, auto
from typing import Any, Dict

from config import influx_config, config as neo_config
from perf_time_functions import time_func_avg, time_func
from queries import *
from utils import get_revision_intervals

from influxdb_client import InfluxDBClient
from py2neo import Graph


# CONFIG
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

def print_influx(result):
    results = []
    for table in result:
        for record in table.records:
            results.append((record.values.get("page_id"), record.get_value()))
    print(results)

class DB(Enum):
    NEO = auto()
    INFLUX = auto()


def exe_general(db_type:DB, connection,query:str, params:Dict[str,Any]=None) -> None :
    """
    This shouldve been a Class, reinventing the wheel here
    """
    if db_type==DB.NEO:
        result = connection.run(query, params)
        if show_data:
            print(result)
    else:
        result = connection.query(org=org,
                query=query,
                params=params
                )
        if show_data:
            print_influx(result)

def exe_return(db_type:DB, connection, query:str, params:Dict[str, Any]=None):
    if db_type == DB.NEO:
        return connection.run(query, params)
    
    return connection.query(org=org,
            query=query,
            params=params
            )


def exe_too_slow(func):
    print("average,"+ func.__name__ + "," + "Too slow")

def print_header():
    print("metric type, query name, time(s)")

# END CONFIG/Boilerplate





# Q1
# Erna_Solberg
Q1_page_id=309272

@time_func_avg
def exe_Q1_neo(graph):
    exe_general(DB.NEO, graph, Q1_neo, {"page_id": Q1_page_id})

@time_func_avg
def exe_Q1_influx(query_api):
    exe_general(
            DB.INFLUX,
            query_api,
            Q1_influx,
            {"page_id":str(Q1_page_id),
                "timestart": datetime.fromtimestamp(0),
                "timestop": datetime.now()})





# Q2
# Erna_Solberg
Q2_page_id=309272
Q2_datestart = datetime(year=2021, month=9, day=1, hour=1, minute=30)
Q2_datestop = datetime(year=2021, month=9, day=1, hour=7, minute=30)

@time_func
def exe_Q2_neo(graph):
    exe_general(DB.NEO, graph, Q2_neo(Q2_datestart.isoformat(), Q2_datestop.isoformat()), {"page_id":Q2_page_id})


@time_func
def exe_Q2_influx(q_api):
    exe_general(DB.INFLUX, q_api, Q2_influx ,{"page_id":Q2_page_id,"timestart":Q2_datestart, "timestop":Q2_datestop})

# Q3
Q3_datestart = datetime(year=2021, month=9, day=1, hour=1, minute=30)
Q3_datestop = datetime(year=2021, month=9, day=1, hour=7, minute=30)

def exe_Q3_neo():
    exe_too_slow(exe_Q3_neo)

@time_func_avg
def exe_Q3_influx(q_api):
    exe_general(DB.INFLUX,
            q_api,
            Q3_influx,
            {"timestart":Q3_datestart, "timestop":Q3_datestop})

# Q4

Q4_datestart = datetime(year=2021, month=9, day=1, hour=1, minute=30)

Q4_datestop = datetime(year=2021, month=9, day=1, hour=7, minute=30)

@time_func_avg
def exe_q4_influx(q_api):
    exe_general(DB.INFLUX, q_api, Q4_influx, {"timestart":Q4_datestart, "timestop":Q4_datestop})

def exe_q4_neo():
    exe_too_slow(exe_q4_neo)

# Q6

Q6_page_id=309272

@time_func_avg
def exe_q6_influx(graph, q_api):
    data = exe_return(DB.NEO, graph, Q6_get_timestamps, {"page_id": Q6_page_id}).data()
    intervals = get_revision_intervals([result["r.from_timestamp"] for result in data])
    for interval in intervals:
        timestart, timestop = interval
        timestart = datetime.fromisoformat(timestart)
        timestop = datetime.fromisoformat(timestop)
        exe_general(DB.INFLUX, q_api, Q6_influx, {"page_id": Q6_page_id, "timestart": timestart, "timestop": timestop})

@time_func_avg
def exe_q6_neo(q_api):
    data = exe_return(DB.NEO, q_api, Q6_get_timestamps, {"page_id": Q6_page_id}).data()
    intervals = get_revision_intervals([result["r.from_timestamp"] for result in data])
    for interval in intervals:
        exe_too_slow(exe_q4_neo)

cypher_pageviews_interval_start = Q4_datestart
cypher_pageviews_interval_end = Q4_datestop

@time_func_avg
def exe_cypher_benchmark(q_api, limit):
    data = exe_return(DB.NEO, q_api, cypher_pageviews_interval(cypher_pageviews_interval_start, cypher_pageviews_interval_end, limit))


if __name__=="__main__":
    query_api = getInfluxClient().query_api()
    graph = getNeo4jDriver()
    """
    print_header()
    exe_Q1_influx(query_api)
    exe_Q1_neo(graph)

    exe_Q2_neo(graph)
    exe_Q2_influx(query_api)

    exe_Q3_neo()
    exe_Q3_influx(query_api)
    
    exe_q4_neo()
    exe_q4_influx(query_api)

    exe_q6_influx(graph, query_api)
    exe_q6_neo(graph)
    """


    for limit in [1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024]:
        exe_cypher_benchmark(graph, limit)