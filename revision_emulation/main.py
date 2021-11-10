from page import Page
from utils.db import store_current_links_to_csv
from utils.config import revisions_config
from utils.csv_helper import CsvHelper
from utils.cypher_generator import RevisionCypherQueryGenerator
from utils.compress_revisions import create_revisions_with_time_interval
import ast

def main():
    if revisions_config["should_create_pagelinks_csv"]:
        print("Creating pagelinks.csv file")
        store_current_links_to_csv()

    if revisions_config["should_create_revisions_csv"]:
        print("Reading pagelinks.csv")
        pages = read_pages()

        print("Creating revisions")
        for page in pages:
            page.generate_revisions()
        
        print("Storing revisions to revisions.csv")
        to_csv = []
        for page in pages:
            to_csv.extend(page.to_csv_format())
        CsvHelper.save_to_csv(to_csv, "revisions.csv")

    if revisions_config["should_create_revision_time_intervals_csv"]:
        print("Creating time intervals")
        create_revisions_with_time_interval()

    if revisions_config["should_create_cypher_queries_csv"]:
        print("Reading time interval .csv")
        intervals = read_intervals()

        print("Creating Cypher queries")
        to_csv = []
        for interval in intervals:
            to_csv.append([RevisionCypherQueryGenerator.insert_revision_with_time_interval(interval)])
        CsvHelper.save_to_csv(to_csv, "interval_insert_queries.csv")


def read_pages():
    """Generates an iterable of Page objects"""
    csv_pages = CsvHelper.read_csv("pagelinks.csv")
    
    pages = []
    for page in csv_pages:
        page_id, page_links = page
        page_links = ast.literal_eval(page_links) # Evaluate page_links as a list and not as a string
        pages.append(Page(page_id, page_links))

    return pages

def read_intervals():
    """Generates an iterable of intervals"""
    csv_intervals = CsvHelper.read_csv("compressed_revisions.csv")

    intervals = [interval for interval in csv_intervals]
    return intervals



if __name__ == "__main__":
    main()