from datetime import datetime
epochDt = datetime.utcfromtimestamp(0)


def dtToEpochScale(dt, scale=1):
    """
    Conert datetime to epoch, scaled by scale
    >>> from datetime import datetime
    >>> dt=datetime.utcfromtimestamp(10)
    >>> dtToEpochScale(dt)
    10.0
    >>> dtToEpochScale(dt, 1000)
    10000.0
    >>> #86400 seconds in a day
    >>> dt2=datetime.strptime("1970-01-02", "%Y-%m-%d")
    >>> dtToEpochScale(dt2, 1)
    86400.0
    """
    delta = dt - epochDt
    return delta.total_seconds() * scale


def dtToEpochSecs(dt):
    """ Convert dt to seconds since epoch, UTC
    >>> from datetime import datetime
    >>> dt=datetime.utcfromtimestamp(10)
    >>> dtToEpochSecs(dt)
    10.0
    >>> dt2=datetime.strptime("1970-01-02", "%Y-%m-%d")
    >>> dtToEpochSecs(dt2)
    86400.0
    """
    return dtToEpochScale(dt, scale=1)


def dtToEpochMsecs(dt):
    """ Convert dt to seconds since epoch, UTC
    >>> from datetime import datetime
    >>> dt=datetime.utcfromtimestamp(10)
    >>> dtToEpochMsecs(dt)
    10000.0
    >>> dt2=datetime.strptime("1970-01-02", "%Y-%m-%d")
    >>> dtToEpochMsecs(dt2)
    86400000.0
    """
    return dtToEpochScale(dt, scale=1000)


def randomTimestampMsec(beginDt, endDt):
    """ Generate a random timestamp between the two passed in dates
        returned timestamp is number of milliseconds since the epoch"""
    import random
    ts = random.randint(dtToEpochMsecs(beginDt), dtToEpochMsecs(endDt))
    return ts


def randomTimestampIsoDate(beginDate, endDate):
    bDt = datetime.strptime(beginDate, "%Y-%m-%d")
    eDt = datetime.strptime(endDate, "%Y-%m-%d")
    print bDt
    print eDt
    if (eDt < bDt):
        raise ValueError("Begin must occur before end")
    return randomTimestampMsec(bDt, eDt)


def nowInEpochMsecs():
    dt = datetime.datetime()
    return dtToEpochMsecs(dt)
