from config import influx_config
from datetime import datetime
from perf_time_functions import time_func_avg, time_func

from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS

from queries import *

bucket="pageviews_b_namespace_eq_0"
org=influx_config["org"]

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

queries = [Q1_neo, Q1_influx]

@time_func_avg
def exe_Q1_neo(query_api):
    result = query_api.query(org=org,  query=Q1_influx, params={"page_id":123,
                                                                "timestart": datetime.fromtimestamp(0),
                                                                "timestop": datetime.now()})
    return result


if __name__=="__main__":
    query_api = getInfluxClient().query_api()
    exe_Q1_neo(query_api)


