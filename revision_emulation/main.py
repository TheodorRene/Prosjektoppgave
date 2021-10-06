from wikisite import WikiSite

def main():
    sites = generate_sites()
    for site in sites:
        site.generate_revisions()
    
    for site in sites:
        for key, entries in site.revisions.items():
            print(site.topic, key, [entry for entry in entries])

def generate_sites():
    """Generates an iterable of Site objects"""
    erna = WikiSite("Erna_Solberg", ["Kaffe", "Norge", "Parlamentet", "Sverige", "Alfabetet"])
    kaffe = WikiSite("Kaffe", ["Norge", "Alfabetet", "Bønner"])
    return [erna, kaffe]


if __name__ == "__main__":
    main()