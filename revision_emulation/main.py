from page import Page
from utils.csv_helper import CsvHelper
from utils.cypher_generator import RevisionCypherQueryGenerator

def main():
    pages = generate_pages()
    for page in pages:
        page.generate_revisions()
    
    to_csv = []
    for page in pages:
        to_csv.extend(page.to_csv_format())
    CsvHelper.save_to_csv(to_csv)

    lines = CsvHelper.read_csv("revisions.csv")
    for line in lines:
        RevisionCypherQueryGenerator.insert_revision(line)

def generate_pages():
    """Generates an iterable of Page objects"""
    erna = Page(1, ["Kaffe", "Norge", "Parlamentet", "Sverige", "Alfabetet"])
    kaffe = Page(2, ["Norge", "Alfabetet", "Bønner"])
    return [erna, kaffe]


if __name__ == "__main__":
    main()