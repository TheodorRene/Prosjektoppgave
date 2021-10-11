from utils.constants import MONTHS, START, END
from utils.random_date import random_date


import random
import math

class WikiSite:
    def __init__(self, topic, newest_revision_links):
        self.topic = topic
        self.revisions = self._initialize_revisions(newest_revision_links)
        self.monthly_update_rate = self._emulate_monthly_update_rate()

    def _initialize_revisions(self, newest_revision_links):
        revisions = { END: newest_revision_links }
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
        timestamps = self.generate_timestamps()
        for index, timestamp in enumerate(timestamps):
            if index != 0:
                copy = [entry for entry in self.revisions[previous_timestamp]]
                self.revisions[timestamp] = WikiSite.simulate_link_updates(copy)

            previous_timestamp = timestamp
        
            
    def generate_timestamps(self):
        """Generate a sorted list with timestamps, including the END timestamp"""
        timestamps = [END]
        for _ in range(self.monthly_update_rate * MONTHS):
            timestamps.append(random_date(START, END))
        timestamps.sort(reverse=True)
        return timestamps

    def to_csv_format(self):
        return [[self.topic, key, value] for (key, value) in self.revisions.items()]



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
        for _ in range(lost_topic_count):
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
        for _ in range(gained_topic_count):
            random_topic = random.choice(ALL_TOPICS)
            if not random_topic in topics:
                topics.append(random_topic)
        return topics

    