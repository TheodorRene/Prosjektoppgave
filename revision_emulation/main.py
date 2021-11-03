from page import Page
from utils.csv_helper import CsvHelper
from utils.cypher_generator import RevisionCypherQueryGenerator
import ast

def main():
    pages = generate_pages()
    for page in pages:
        page.generate_revisions()
    
    to_csv = []
    for page in pages:
        to_csv.extend(page.to_csv_format())
    CsvHelper.save_to_csv(to_csv, "revisions.csv")

    lines = CsvHelper.read_csv("revisions.csv")
    for line in lines:
        RevisionCypherQueryGenerator.insert_revision(line)

def generate_pages():
    """Generates an iterable of Page objects"""
    csv_pages = CsvHelper.read_csv("pagelinks.csv")
    
    pages = []
    for page in csv_pages:
        page_id, page_links = page
        page_links = ast.literal_eval(page_links) # Evaluate page_links as a list and not as a string
        pages.append(Page(page_id, page_links))

    return pages


if __name__ == "__main__":
    main()