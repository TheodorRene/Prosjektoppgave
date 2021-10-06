from utils import random_date

import random
import math

class WikiSite:
    def __init__(self, topic, newest_revision_links):
        self.topic = topic
        self.revisions = self._initialize_revisions(newest_revision_links)
        self.monthly_update_rate = self._emulate_monthly_update_rate()

    def _initialize_revisions(self, newest_revision_links):
        revisions = { "newest": newest_revision_links }
        return revisions 
    
    def _emulate_monthly_update_rate(self):
        """
        Set a monthly update rate for the page.
        TODO: Create a module for distributing update rates from real Wikipedia articles 
        """
        return 10

    def generate_revisions(self):
        """
        Generate revisions based on the monthly update rate
        TODO: Generate for a given time frame. Currently only calculates for one month.
        """
        MONTHS = 1
        START = "2021-01-01T00:00:01"
        END = "2021-02-01T23:59:59"
        for i in range(self.monthly_update_rate * MONTHS):
            revision_timestamp = random_date(START, END)
            copy = [entry for entry in self.revisions["newest"]]
            self.revisions[revision_timestamp] = WikiSite.simulate_link_updates(copy)

    @staticmethod
    def simulate_link_updates(newest_topics):
        new_topics = WikiSite.simulate_link_loss(newest_topics)
        new_topics = WikiSite.simulate_link_gain(new_topics)
        return new_topics

    @staticmethod
    def simulate_link_loss(topics):
        """
        Simulate a link loss based on a probability of losing a link
        TODO: Define a link loss probability distribution based on real data set
        """
        LINK_LOSS_PROBABILITY = 0.1
        lost_topic_count = math.ceil(LINK_LOSS_PROBABILITY * len(topics))
        for i in range(lost_topic_count):
            random_topic = random.choice(topics)
            topics.remove(random_topic)
        return topics

    @staticmethod
    def simulate_link_gain(topics):
        """
        Simulate link gain based on a probability of gaining a random link
        TODO: Define whith topics can be linked. 
        """
        ALL_TOPICS = ["Sau", "Geit", "Danmark", "Finland", "Finnmark", "Viken", "Belgia", "Paris", "Madrid", "Støre"]
        LINK_GAIN_PROBABILITY = 0.1
        gained_topic_count = math.ceil(LINK_GAIN_PROBABILITY * len(topics))
        for i in range(gained_topic_count):
            random_topic = random.choice(ALL_TOPICS)
            if not random_topic in topics:
                topics.append(random_topic)
        return topics

    