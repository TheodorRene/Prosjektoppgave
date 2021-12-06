def parse_multiple_id_regex(ids):
    """Parses ids [x,y,x] to the form "/^(x|y|z)$/"""
    string_ids = [str(id) for id in ids]
    pipe_separated_ids = "|".join(string_ids)
    return f"/^({pipe_separated_ids})$/"
