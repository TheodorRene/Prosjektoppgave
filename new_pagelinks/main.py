from wiki_page_revision_set import WikiPageRevisionSet

import requests

def __main__():
    session = requests.Session()

    start_page = WikiPageRevisionSet("Erna Solberg", 1, request_session=session)
    topics_seen = [start_page.topic]

    topics = {}
    for revision in start_page.revisions:
        topics = {**topics, **revision.links}
    
    for topic in topics.keys():
        if topic not in topics_seen:
            print(topic)
            page_revision_set = WikiPageRevisionSet(topic, 2, request_session=session)
            topics_seen.append(page_revision_set.topic)


    print(len(topics_seen))


    """    
    pages = [start_page]
    pages_seen = []

    start_page.get_revisions()
    """


if __name__ == "__main__":
    __main__()
