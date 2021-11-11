from datetime import datetime
import random
from string import ascii_uppercase
from sys import argv

from neo4j import GraphDatabase

from config import config as c

def log(text):
    print(datetime.now(), text)


#IMPURE
def insert_line_neo4j(tx, parsed_tuple, hourly_counts)->None:
    """
    Actually insert the pageview data
    :param parsed_tuple: A parsed tuple
    :param hourly_counts: [(hour,count)] List of tuples with hourly pageviews
    :return: None, only sideffects
    """
    assert len(parsed_tuple) == 5
    (wiki_code, article_title, page_id, _, _) = parsed_tuple
    add_inital_dummy_head(tx, page_id)
    for (timestamp, count) in hourly_counts:
        # TODO use dict
        result=tx.run(append_pageview_better_q, wiki_code=wiki_code, article_title=article_title, page_id=page_id, timestamp=timestamp, count=count)

#IMPURE
def add_inital_dummy_head(tx, page_id):
    result = tx.run(add_inital_dummy_head_q, page_id=page_id)

#IMPURE
def delete_all_pageviews(tx):
    """
    Delete all pageviews
    """
    tx.run(reset_state_q)


reset_state_q= "" + \
              ("MATCH (pv:PageView) DETACH DELETE pv ")


"""
This is like adding an empty list to an object. We need something to append to when using append_pageview{_better}
"""
# ┌────────┐              ┌─────────────────────┐
# │  Page  ├──:FIRST───►  │Pageview{dummy:True} │
# │        │              └─────────────────────┘
# └───┬────┘                   ▲
#     │                        │
#     └─────────:LAST──────────┘
add_inital_dummy_head_q = "" + \
                        ("MATCH (p:Page{id:$page_id}) "
                        "WHERE NOT (p) -[:FIRST]-> () AND NOT (p) -[:LAST]-> () "  # Make sure there isnt already a head or tail
                        "CREATE (p) -[:FIRST] -> (head:PageView{dummy:True}), "
                        "(p) -[:LAST] -> (head) ")


# ┌─────┐             ┌─────────────────────┐
# │Page ├───:FIRST────► PageView{dummy:True}│
# └──┬──┘             └──────────┬──────────┘
#    │                           │
#    │                          :NEXT
#    │                           │
#    │                ┌──────────▼─────────────┐
#    └─:LAST──────────►PageView{wiki_code: ...}│
#                     └────────────────────────┘
# O(1) inserts
append_pageview_better_q = "" + \
        ("MATCH (p:Page{id:$page_id}) -[r:LAST] -> (l:PageView) " # Match to get tail of linked list
        "CREATE (a:PageView) "                                         # Create Pageview
        "SET a.wiki_code=$wiki_code "
        "SET a.article_title=$article_title "
        "SET a.page_id=$page_id "
        "SET a.timestamp=datetime($timestamp) "
        "SET a.count=$count "
        "CREATE (l) -[:NEXT]-> (a), "                                    # Creat link between last tail and new tail
        "(p) -[:LAST]-> (a) "                                            # Create tail relation to new tail
        "DELETE (r) ")                                                   # Delete tail relation to old tail

#PURE
def generate_ssv_for_line(parsed_tuple, hourly_counts):
    """
    Converts our originally parsed tuple into a list of strings with space separated values using hourly_counts
    """
    (wiki_code, article_title, page_id, _, _) = parsed_tuple
    data = []
    for (timestamp, count) in hourly_counts:
        data.append(" ".join([wiki_code, article_title, str(page_id), timestamp, str(count)]))
    return data


#PURE
def parse_hourly_counts(tuple, filename):
    """
    Convert hourly counts into list of tuples
    :param tuple: parsed tuple with length 5
    :param filename: Filename that contains the dateinfo, needed for
    transforming an hour to timestamp
    :returns: [(timestamp,count)] the timestamp(iso-format) and the number of hits for that hour in a tuple
    """
    (_, _, _, _, hourly_counts) = tuple
    alphabet = list(ascii_uppercase)

    num = ""
    cur_hour = 0
    first = True
    list_of_tuples = []
    for char in hourly_counts:
        if first:
            assert char.isalpha()
            cur_hour = alphabet.index(char)
            first = False
        elif char.isdigit():
            num += char
        else:
            assert num != ""
            assert char.isalpha()
            list_of_tuples.extend(get_minute_views(cur_hour, filename, int(num)))
            cur_hour = alphabet.index(char)
            num = ""
    list_of_tuples.extend(get_minute_views(cur_hour, filename, int(num)))
    return list_of_tuples


#PURE
def get_minute_views(hour, filename, num):
    """
    Randomly break down the hourly page views to minutes.
    :param hour: Hour of the day [0..23]
    :param filename: Contains the date, assumes the file format matches "pageview_YYYYMMDD"
    :return: [(timestamp, count)] the timestamp(iso-format) and the number of hits for that minute in a tuple
    """
    tuples = []
    minutes = [0 for i in range(60)]
    for _ in range(num):
        minutes[random.randint(0, 59)] += 1
    for minute, minute_count in enumerate(minutes):
        if minute_count:
            tuples.append((get_timestamp(hour, minute, filename), minute_count))
    return tuples


#PURE
def get_timestamp(hour, minute, filename)->str:
    """
    Get the timestamp in iso8601 format(YYYY-MM-DDTHH:MM:SS) using the hour and
    name of the file
    :param hour: Hour of the day [0..23]
    :param filename: Contains the date, assumes the file format matches "pageview_YYYYMMDD"
    :return: timestamp
    """
    date = filename.split("_")[1]
    d = datetime(year=int(date[0:4]), month=int(date[4:6]), day=int(date[6:8]), hour=hour, minute=minute)
    return d.isoformat()



#PURE
def parse_line(line):
    """
    Parse a line from the file.
    :param line: A line to be parsed
    :returns: None, if line is unparsable
    :returns: Tuple with lenght 6 if parsable
    """
    try:
        (wiki_code, article_title, page_id,
         user_agent, daily_total, hourly_counts) = line.strip().split(" ")
        predicates = []
        if c["only_norway"]:
            predicates.append(wiki_code == "no.wikipedia")
        if c["only_pages"]:
            predicates.append(not article_title.startswith("Fil:"))
        if c["only_include_desktop"]:
            predicates.append( user_agent=="desktop" )
        return (wiki_code, article_title, int(page_id), daily_total, hourly_counts) if all(predicates) else None
    except:
        if c["debug"]:
            print("Could not parse this line", line)
        return


#IMPURE
def do_job(filepath):
    with open(filepath, "r") as f:
        if not c["dry_run"]:
            log("Connecting to Neo4j db")
            driver = GraphDatabase.driver(c["uri"], auth=(c["user"], c["password"]))

        #should in theory both work for "dump/pageview_XXXXX" and "pageview_xxxx"
        filename=filepath.split("/")[-1]
        if c["debug"]:
            log("Iterating through lines")
        for line in f:
            parsed = parse_line(line)
            if parsed:
                hourly_counts = parse_hourly_counts(parsed, filename)
                if c["dry_run"]:
                    ssv = generate_ssv_for_line(parsed,hourly_counts)
                    [print(el) for el in ssv]
                else:
                    with driver.session() as s:
                        s.write_transaction(insert_line_neo4j, parsed, hourly_counts)
            elif c["debug"]:
                print("DID NOT PARSE MOST LIKELY BECAUSE OF CONFIG:", line)


if __name__ == "__main__":
    filepath = argv[1]
    if c["debug"]:
        log("Starting script")
    do_job(filepath)
    if c["debug"]:
        log("finito")
"""
if __name__ == "__main__":
    tuple = ("", "", "", "", "A1B2C3D4E62F26G125H612I16J74K2457L885M24N24O8P45Q245R1S2T4U5V6W7")
    [print(el) for el in parse_hourly_counts(tuple, "_20000101")]
"""
