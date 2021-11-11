import random
import time

def random_date(start, end, time_format='%Y-%m-%dT%H:%M:%S'):
    """Get a time between a range of two formatted times.

    start and end should be strings specifying times formatted in the
    given format (strftime-style), giving an interval [start, end].
    The returned time will be in the specified format.

    https://stackoverflow.com/questions/553303/generate-a-random-date-between-two-other-dates
    """

    stime = time.mktime(time.strptime(start, time_format))
    etime = time.mktime(time.strptime(end, time_format))

    ptime = stime + random.random() * (etime - stime)

    return time.strftime(time_format, time.localtime(ptime))