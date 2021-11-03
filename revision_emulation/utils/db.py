import os
import mysql.connector

from csv_helper import CsvHelper

c = {
    "host":"localhost",
    "user": os.environ.get("MARIADB_USER"),
    "password": os.environ.get("MARIADB_ROOT_PASSWORD"),
    "database":"pagelinks-db",
}

def get_all_link_titles():
    #TODO: Remove config as it is set in #6
  
    db = mysql.connector.connect(
        host=c["host"],
        user=c["user"],
        password=c["password"],
        database=c["database"]
    )
    cursor = db.cursor()

    query = "SELECT DISTINCT from_page_id from pagelinks_complete;"
    cursor.execute(query)

    result = cursor.fetchall()
    rows = [[row[0] for row in result]]
    CsvHelper.save_to_csv(rows, "all_titles.csv")

def store_current_links_to_csv():
    db = mysql.connector.connect(
        host=c["host"],
        user=c["user"],
        password=c["password"],
        database=c["database"]
    )

    cursor = db.cursor()

    query = "SELECT * FROM pagelinks_complete ORDER BY from_page_id;"
    cursor.execute(query)

    result = cursor.fetchall()

    to_csv = []
    previous_page_id = None
    current_page = PagelinkSet()
    for row in result:
        new_page_id = row[0]

        if is_new_page(previous_page_id, new_page_id):
            if previous_page_id:
                to_csv.append(current_page.to_representation())
            
            current_page = PagelinkSet(new_page_id)
 
        current_page.link_ids.append(row[2])
        previous_page_id = new_page_id
    to_csv.append(current_page.to_representation())

    CsvHelper.save_to_csv(to_csv, "pagelinks.csv")

def is_new_page(previous_page_id, new_page_id):
    return previous_page_id != new_page_id
    
class PagelinkSet:
    def __init__(self, id=None):
        self.id = id
        self.link_ids = []

    def to_representation(self):
        return [self.id, self.link_ids]
    
def main():
    get_all_link_titles()

if __name__ == "__main__":
    main()


