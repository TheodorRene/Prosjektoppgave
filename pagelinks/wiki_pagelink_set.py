import requests
import re

class WikiPagelinkSet:
    """ Class containing all internal Wikipedia links for a single Wikipedia site. """

    def __init__(self, topic, depth):
        """ Initialize by translating topic to be on a WikiAPI accepted format. """
        self.topic = topic.replace(" ", "_")
        self.links = {}
        self.depth = depth

    def add_link(self, link):
        try:
            self.links[link] += 1
        except KeyError:
            self.links[link] = 0
    
    def get_links_from_wiki(self):
        """ Send request to Wikipedia API, parse and get all internal wikipedia links. """
        url = f"""https://en.wikipedia.org/w/api.php?action=query&format=json&prop=revisions&rvlimit=20&titles={self.topic}&rvlimit=max&rvprop=timestamp|content"""

        response = requests.get(url)
        pages = response.json()["query"]["pages"]
        page_id = list(pages.keys())[0]

        try:
            revisions = pages[page_id]["revisions"]
        except KeyError:
            return

        link_element_regex = r"\[\[([a-zA-Z0-9 ]*)[\|\]]"
        matches = re.findall(link_element_regex, revisions[0]["*"])
        for match in matches:
            self.add_link(match)

    def propagate(self):
        """
        Get links and send new requests for all links encountered.

        TODO: Batch requests in order to bump up depth.
        See https://www.mediawiki.org/wiki/API:Etiquette for more information.
        """

        if self.depth >= 3:
            return []

        self.get_links_from_wiki()

        next_pages = []
        for link in self.links.keys():
            link_revision = WikiPagelinkSet(link, self.depth + 1)
            next_pages.append(link_revision)

        return next_pages
