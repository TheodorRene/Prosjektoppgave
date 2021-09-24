from neo4j import GraphDatabase


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


def insert_line_neo4j(tx, line):
    assert len(line) == 5
    (wiki_code, article_title, page_id, daily_total, hourly_counts) = line
    if wiki_code != "no.wikipedia":
        return
    print("INSERTING")
    tx.run("CREATE (a: PageView )"
           "SET a.wiki_code=$wiki_code " 
           "SET a.article_title=$article_title "
           "SET a.page_id=$page_id "
           "SET a.daily_total=$daily_total "
           "SET a.hourly_counts=$hourly_counts ", wiki_code=wiki_code, article_title=article_title, page_id=page_id, daily_total=daily_total, hourly_counts=hourly_counts)


def parse_line(line):
    try:
        (wiki_code, article_title, page_id,
         daily_total, hourly_counts) = line.split(" ")
        return (wiki_code, article_title, page_id, daily_total, hourly_counts)
    except:
        return


def do_job(tx, filename):
    with open(filename, "r") as f:
        for line in f:
            parsed = parse_line(line)
            if parsed:
                insert_line_neo4j(tx, parsed)


if __name__ == "__main__":
    greeter = HelloWorldExample("bolt://localhost:7687", "neo4j", "s3cr3t")
    driver = greeter.driver
    filename = input("Filename:")
    with driver.session() as s:
        s.write_transaction(do_job, filename)
