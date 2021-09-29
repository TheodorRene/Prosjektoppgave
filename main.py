from neo4j import GraphDatabase
from string import ascii_uppercase
from sys import argv

debug = True


class HelloWorldExample:

    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    def print_greeting(self, message):
        with self.driver.session() as session:
            greeting = session.write_transaction(
                self._create_and_return, message)
            print(greeting)

    @staticmethod
    def _create_and_return(tx, message):
        result = tx.run("CREATE (a:Greeting) "
                        " SET a.message = $message "
                        " RETURN a.message +', from node' + id(a)", message=message)
        return result.single()[0]


def insert_line_neo4j(tx, parsed_tuple, hourly_counts):
    assert len(parsed_tuple) == 5
    (wiki_code, article_title, page_id, daily_total, _) = parsed_tuple
    if debug:
        print("INSERTING")
    for (hour, count) in hourly_counts:
        tx.run("CREATE (a: PageView )"
               "SET a.wiki_code=$wiki_code "
               "SET a.article_title=$article_title "
               "SET a.page_id=$page_id "
               "SET a.timestamp=$hour "
               "SET a.count=$count ", wiki_code=wiki_code, article_title=article_title, page_id=page_id, hour=hour, count=count)


def parse_hourly_counts(tuple):
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
    try:
        (wiki_code, article_title, page_id,
         daily_total, hourly_counts) = line.strip().split(" ")
        #return (wiki_code, article_title, page_id, daily_total, hourly_counts) if wiki_code == "no.wikipedia" else None
        return (wiki_code, article_title, page_id, daily_total, hourly_counts) 
    except:
        return


def do_job(tx, filename):
    with open(filename, "r") as f:
        for line in f:
            parsed = parse_line(line)
            if parsed:
                hourly_counts = parse_hourly_counts(parsed)
                insert_line_neo4j(tx, parsed, hourly_counts)
            elif debug:
                print("DID NOT PARSE:", line)


if __name__ == "__main__":
    greeter = HelloWorldExample("bolt://localhost:7687", "neo4j", "s3cr3t")
    driver = greeter.driver
    filename = argv[1]
    with driver.session() as s:
        s.write_transaction(do_job, filename)
