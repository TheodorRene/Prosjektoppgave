from wiki_page_revision_set import WikiPageRevisionSet

import grequests
import re

START_DATE = "2021-01-01T00:00:01"
END_DATE = "2021-06-30T23:59:59"
WIKIPEDIA_API_URL = "https://en.wikipedia.org/w/api.php"

def create_urls(topic):
    return  f"https://en.wikipedia.org/w/api.php?action=query&format=json&prop=revisions&rvlimit=max&titles={topic}&rvprop=timestamp|ids|content&rvstart=2021-06-30T23:59:59&rvend=2021-01-01T00:00:01"

def process(response):
    try:
        revisions = parse_revisions(response)
        return revisions
    except DeadLinkException:
        return None

def parse_revisions(response):
    topic_id = self.get_topic_id(response)
    if topic_id == "-1":
        raise DeadLinkException

    revisions = []
    page = get_page(response)
    try:
        page_revisions = page.json()["query"]["pages"][topic_id]["revisions"]
    except KeyError:
        return # The link is capitalized the wrong way in the API
        
    for revision in page_revisions:
        try:
            links = {}
            link_element_regex = r"\[\[([a-zA-Z0-9 ]*)[\|\]]"

            matches = re.findall(link_element_regex, content=revision["*"])
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

def get_page(self, response):
    if "continue" in response.json():
       rvcontinue = response.json()["continue"]["rvcontinue"]
       return f"https://en.wikipedia.org/w/api.php?action=query&format=json&prop=revisions&rvlimit=max&titles={topic}&rvprop=timestamp|ids|content&rvstart=2021-06-30T23:59:59&rvend=2021-01-01T00:00:01&rvcontinue="
    pass

def __main__():
    topic_queue = ["Norway", "Sweden", "Denmark", "Belgium", "Finland", "Iceland", "Bulgaria", "Cyprus", "Germany", "France"]
    pagination_queue = []

    while topic_queue:
        next_topics, topic_queue = (topic_queue[:10], topic_queue[10:])
        next_urls = generate_urls(next_topics)
        responses = send_batch_requests(next_urls)

        add_new_content(topic_queue, responses)
        next_pages = get_next_pages(responses)
        while next_pages:
            responses = send_batch_requests(next_pages)
            add_new_content(topic_queue, responses)
            next_pages = get_next_pages(response)

    # Må si hva hver post peker på. Kan lage en dictionary topic->revisionset




    topics = {}
    for revision in start_page.revisions:
        topics = {**topics, **revision.links}
    
    urls = [create_urls(topic) for topic in topics.keys()]

    responses = grequests.map(grequests.get(url) for url in urls)
    print(responses[0].content)

    while urls:
        urls = [process(response) for response in responses]
        responses = grequests.map(grequests.get(url) for url in urls)


    """
    for topic in topics.keys():
        if topic not in topics_seen:
            print(topic)
            page_revision_set = WikiPageRevisionSet(topic, 2, request_session=session)
            topics_seen.append(page_revision_set.topic)


    print(len(topics_seen))
    """

if __name__ == "__main__":
    __main__()
