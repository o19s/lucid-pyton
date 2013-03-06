

def getDocs(queryResult):
    try:
        return queryResult['QUERY']['json']['response']['docs']
    except KeyError:
        raise IOError(str(queryResult))


def getGroups(queryResult):
    try:
        return queryResult['QUERY']['json']['grouped'][1]['groups']
    except KeyError:
        raise IOError(str(queryResult))
