from neo4j import GraphDatabase
from string import ascii_uppercase
from sys import argv
from config import config as c


def insert_line_neo4j(tx, parsed_tuple, hourly_counts)->None:
    """
    Actually insert the data
    :param parsed_tuple: A parsed tuple
    :param hourly_counts: [(hour,count)] List of tuples with hourly pageviews
    :return: None, only sideffects
    """
    assert len(parsed_tuple) == 6
    (wiki_code, article_title, page_id, _, _, _) = parsed_tuple
    if c["debug"]:
        print("INSERTING")
    for (hour, count) in hourly_counts:
        tx.run("CREATE (a: PageView )"
               "SET a.wiki_code=$wiki_code "
               "SET a.article_title=$article_title "
               "SET a.page_id=$page_id "
               "SET a.timestamp=$hour "
               "SET a.count=$count ", wiki_code=wiki_code, article_title=article_title, page_id=page_id, hour=hour, count=count)

def generate_ssv_for_line(parsed_tuple, hourly_counts):
    """
    Converts our originally parsed tuple into a list of strings with space separated values using hourly_counts
    """
    (wiki_code, article_title, page_id, _, _, _) = parsed_tuple
    data = []
    for (hour, count) in hourly_counts:
        data.append(" ".join([wiki_code, article_title, page_id, str(hour), str(count)]))
    return data


def parse_hourly_counts(tuple):
    """
    Convert hourly counts into list of tuples
    :param tuple: parsed tuple with length 5
    :returns: [(hour,count)] the hour and the number of hits for that hour in a tuple
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
            list_of_tuples.append((cur_hour, num))
            cur_hour = alphabet.index(char)
            num = ""
    list_of_tuples.append((cur_hour, num))
    return list_of_tuples


def parse_line(line):
    """
    Parse a line from the file.
    :param line: A line to be parsed
    :returns: None, if line is unparsable
    :returns: Tuple with lenght 6 if parsable
    """
    try:
        (wiki_code, article_title, page_id,
         daily_total,_, hourly_counts) = line.strip().split(" ")
        predicates = []
        if c["only_norway"]:
            predicates.append(wiki_code == "no.wikipedia")
        if c["only_pages"]:
            predicates.append(not article_title.startswith("Fil:"))
        return (wiki_code, article_title, page_id, daily_total, hourly_counts) if all(predicates) else None
    except:
        print("Could not parse this line", line)
        return


def do_job(filename):
    with open(filename, "r") as f:
        if not c["dry_run"]:
            driver = GraphDatabase.driver(c["uri"], auth=(c["user"], c["password"]))

        for line in f:
            parsed = parse_line(line)
            if parsed:
                hourly_counts = parse_hourly_counts(parsed)
                if c["dry_run"]:
                    ssv = generate_ssv_for_line(parsed,hourly_counts)
                    [print(el) for el in ssv]
                else:
                    with driver.session() as s:
                        s.write_transaction(insert_line_neo4j, parsed, hourly_counts)
            elif c["debug"]:
                print("DID NOT PARSE:", line)



if __name__ == "__main__":
    filename = argv[1]
    do_job(filename)
