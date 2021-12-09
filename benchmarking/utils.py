from typing import List

def parse_multiple_id_regex(ids):
    """Parses ids [x,y,x] to the form "/^(x|y|z)$/"""
    string_ids = [str(id) for id in ids]
    pipe_separated_ids = "|".join(string_ids)
    return f"/^({pipe_separated_ids})$/"

def get_revision_intervals(timestamps:List[str]) -> List[tuple]:
    return [(timestamps[i], timestamps[i+1]) for i in range(0, len(timestamps)-1)]