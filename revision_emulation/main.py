from wikisite import WikiSite
from utils.csv_helper import CsvHelper
from utils.cypher_generator import RevisionCypherQueryGenerator

def main():
    sites = generate_sites()
    for site in sites:
        site.generate_revisions()
    
    to_csv = []
    for site in sites:
        to_csv.extend(site.to_csv_format())
    CsvHelper.save_to_csv(to_csv)


    lines = CsvHelper.read_csv("revisions.csv")
    for line in lines:
        RevisionCypherQueryGenerator.insert_revision(line)

def generate_sites():
    """Generates an iterable of WikiSite objects"""
    erna = WikiSite("Erna_Solberg", ["Kaffe", "Norge", "Parlamentet", "Sverige", "Alfabetet"])
    kaffe = WikiSite("Kaffe", ["Norge", "Alfabetet", "Bønner"])
    return [erna, kaffe]


if __name__ == "__main__":
    main()