from utils.csv_helper import CsvHelper
import ast

class PagelinkValidity:
    def __init__(self, interval_start, interval_end):
        self.interval_start = interval_start
        self.interval_end = interval_end

    def update(self, timestamp):
        self.interval_start = timestamp

    def to_representation(self):
        return f"{self.interval_start},{self.interval_end}"

class PagelinkIntervalMap:
    def __init__(self):
        self.map = {}

    def update(self, new_timestamp, previous_timestamp, revisions):
        """Update"""
        self._create_or_update(new_timestamp, previous_timestamp, revisions)
        return self._get_purged_records(new_timestamp)
        

    def _create_or_update(self, new_timestamp, previous_timestamp, revisions):
        for page_link_id in revisions:
            if page_link_id in self.map.keys():
                self.map[page_link_id].update(new_timestamp)

            else:
                self.map[page_link_id] = PagelinkValidity(new_timestamp, previous_timestamp)
    
    def _get_purged_records(self, new_timestamp):
        """
        Remove and return all pages which were not in the current revision.
        Returns a list on the form [ [id, start, end], ... ] 
        """
        purged_records = []
        pages_to_pop = []
        for page_id, interval in self.map.items():
            if not interval.interval_start == new_timestamp:
                purged_records.append([page_id, interval.to_representation()])
                pages_to_pop.append(page_id)
        [self.map.pop(page) for page in pages_to_pop]
        return purged_records

    def purge_all(self):
        return [[page_id, interval.to_representation()] for page_id, interval in self.map.items()]


def _prefix_with_from_page_id(from_page_id, purged_intervals):
    """Prefixes the purged interval records with the page id of the current page."""
    result = []
    for interval in purged_intervals:
        relation = [int(from_page_id)]
        relation.extend(interval)
        result.append(relation)
    return result

def create_revisions_with_time_interval():
    lines = CsvHelper.read_csv("revisions.csv")
    first = True
    previous_id = None
    previous_timestamp = None
    page_link_intervals = None
    to_csv = []
    for line in lines:
        new_id, new_timestamp, revisions = line
        revisions = ast.literal_eval(revisions) # Evaluate revisions as a list and not as a string
        
        if first:
            page_link_intervals = PagelinkIntervalMap()
            page_link_intervals.update(new_timestamp, previous_timestamp, revisions)
            previous_id = new_id
            previous_timestamp = new_timestamp
            first = False
            continue

        if previous_id == new_id:
            purged_intervals = page_link_intervals.update(new_timestamp, previous_timestamp, revisions)
            to_csv.extend(_prefix_with_from_page_id(new_id, purged_intervals))
        
        else:
            purged_intervals = page_link_intervals.purge_all()
            to_csv.extend(_prefix_with_from_page_id(previous_id, purged_intervals))
            previous_timestamp = None
            page_link_intervals = PagelinkIntervalMap()
            page_link_intervals.update(new_timestamp, previous_timestamp, revisions)
        
        previous_id = new_id
        previous_timestamp = new_timestamp
    
    purged_intervals = page_link_intervals.purge_all()
    to_csv.extend(_prefix_with_from_page_id(new_id, purged_intervals))
    
    CsvHelper.save_to_csv(to_csv, "compressed_revisions.csv")