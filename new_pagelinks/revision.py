import re


class Revision:
    def __init__(self, revision_id, timestamp, content, topic):
        self.revision_id = revision_id
        self.timestamp = timestamp
        self.topic = topic
        self.links = {}
        self.generate_links(content)

    def __str__(self):
        return f"{self.revision_id} for {self.topic} at {self.timestamp} with links {str(self.links)}"

    def generate_links(self, content):
        links = {}
        link_element_regex = r"\[\[([a-zA-Z0-9 ]*)[\|\]]"

        matches = re.findall(link_element_regex, content)

        for link in matches:
            self.links[link] = True
    


