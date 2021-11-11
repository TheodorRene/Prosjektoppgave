from dataclasses import dataclass
from datetime import datetime

from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS

from config import influx_config as c

def debuglog(text):
    """
    Why use 5 minutes to learn pythons logging library that will help you for
    the rest of your career when you can make your own in like 10 minutes
    """
    if c["debug"]:
        print(datetime.now(), text)

bucket="pageviews_b"
org=c["org"]

def getClient():
    debuglog("Connecting to influx")
    return InfluxDBClient(
        url=c["url"],
        token=c["token"],
        org=c["org"]
        )

def getWriteApi(client):
    return client.write_api(write_options=SYNCHRONOUS)

def getQueryApi(client):
    return client.query_api()

def getPoint(pageview):
    """
    InfluxDB uses Point as the object to be inserted in the database
    :input: pageview: PageView
    :return: Point
    """
    return (Point("pageview")
            .tag("page_id",pageview.page_id)
            .field("hits", pageview.view_count)
            .time(pageview.timestamp))

def insertPoint(write_api, point):
    write_api.write(bucket=bucket, org=org, record=point)

@dataclass(frozen=True)
class PageView:
    wiki_code: str
    article_title: str
    page_id: int
    timestamp: datetime
    view_count: int

def parse_line(line):
    try:
        (wiki_code, article_title, page_id, timestamp, view_count) = line.split(" ")
        return PageView(wiki_code, article_title, int(page_id), datetime.fromisoformat(timestamp), int(view_count))
    except:
        debuglog("Could not parse: " + line)

def do_job(write_api, filepath):
    with open(filepath, "r") as f:
        for line in f:
            pageview=parse_line(line)
            point=getPoint(pageview)
            if not c["dry_run"]:
                insertPoint(write_api, point)
            else:
                print(point)

number_of_hits_for_a_range_page_id = "" + \
    (f'from(bucket: "{bucket}")'
       '|> range(start: timestart, stop: timestop)'
       '|> filter(fn: (r) => r["_measurement"] == "pageview")'
       '|> filter(fn: (r) => r["_field"] == "hits")'
       '|> filter(fn: (r) => r["page_id"] == page_id)'
       '|> sum(column: "_value")')

def do_query(query_api, query, params):
    debuglog("doing query " +  query)
    result = query_api.query(org=org,  query=query, params=params)

    results = []
    for table in result:
            for record in table.records:
                        results.append((record.get_field(), record.get_value()))
    return results

day_start=datetime(year=2021, month=9, day=1, hour=0, minute=1)
day_end=datetime(year=2021, month=9, day=1, hour=23, minute=59)

def get_payload(page_id):
    return {
        "page_id":page_id,
        "timestart":day_start,
        "timestop":day_end
    }

from sys import argv
if __name__=="__main__":
    client = getClient()
    write_api = getWriteApi(client)
    query_api = getQueryApi(client)
    if c["query"]:
        print(do_query(query_api, number_of_hits_for_a_range_page_id, get_payload("1000275")))
    else:
        filepath = argv[1]
        do_job(write_api, filepath)
    write_api.close()



