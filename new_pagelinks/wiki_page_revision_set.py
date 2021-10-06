import grequests
import re

from revision import Revision
from utils.exceptions.dead_link_exception import DeadLinkException


START_DATE = "2021-01-01T00:00:01"
END_DATE = "2021-06-30T23:59:59"
WIKIPEDIA_API_URL = "https://en.wikipedia.org/w/api.php"


class WikiPageRevisionSet:
    """ Class containing all internal Wikipedia links for a single Wikipedia site. """

    def __init__(self, topic, depth):
        """ Initialize by translating topic to be on a WikiAPI accepted format. """
        self.topic = topic.replace(" ", "_")
        self.revisions = [] #self.get_revisions()
        self.next_url = create_url()
        self.depth = depth

    def create_url(self, rvcontinue=None):
        if rvcontinue:
            return f"https://en.wikipedia.org/w/api.php?action=query&format=json&prop=revisions&rvlimit=max&titles={self.topic}&rvprop=timestamp|ids|content&rvstart=2021-06-30T23:59:59&rvend=2021-01-01T00:00:01&rvcontinue={rvcontinue}"
        return  f"https://en.wikipedia.org/w/api.php?action=query&format=json&prop=revisions&rvlimit=max&titles={self.topic}&rvprop=timestamp|ids|content&rvstart=2021-06-30T23:59:59&rvend=2021-01-01T00:00:01"


    def get_revisions(self):
        """
        Get revision data for a single topic.
        NOTE: The revision timelime is backwards, hence rvstart is the END_DATE.
        """        
        rs = (grequests.get(u) for u in [self.create_urls()])
        response = grequests.map(rs)
        try:
            revisions = self.parse_revisions(response[0])
            return revisions
        except DeadLinkException:
            return None
    
    def parse_revisions(self, response):
        """
        Create Revision objects out of the response
        """
        print(type(response.json()))
        topic_id = self.get_topic_id(response)
        if topic_id == "-1":
            raise DeadLinkException

        revisions = []
        for page in self.get_pages(response):
            try:
                page_revisions = page.json()["query"]["pages"][topic_id]["revisions"]
            except KeyError:
                continue # The link is capitalized the wrong way in the API
            
            for revision in page_revisions:

                try:
                    revisions.append(
                        Revision(
                            revision_id=revision["revid"],
                            timestamp=revision["timestamp"], 
                            content=revision["*"],
                            topic=self.topic
                        )
                )
                except KeyError:
                    continue # Content is hidden https://en.wikipedia.org/w/api.php?action=query&format=json&prop=revisions&rvlimit=1&titles=Beijing&rvslots=*&rvprop=content&rvstartid=1014076205
        return revisions
        

    def get_topic_id(self, response):
        """
        Get the topic ID of the current topic.
        There is only a single topic ID for each request unless several topics are requested.
        """
        pages = response.json()["query"]["pages"]
        return list(pages.keys())[0]


    def get_pages(self, response):
        pages = [response]
        params = self._get_default_params()
        while "continue" in response.json():
            params["rvcontinue"] = response.json()["continue"]["rvcontinue"]
            response = grequests.map([grequests.get(WIKIPEDIA_API_URL, data=params)])
            pages.append(response)

        return pages


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
            link_revision = WikiPagelinkSet(link, self.depth + 1, self.t)
            next_pages.append(link_revision)

        return next_pages
