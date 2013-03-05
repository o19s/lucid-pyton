

def getDocs(queryResult):
    try:
        return queryResult['QUERY']['json']['response']['docs']
    except KeyError:
        raise IOError(str(queryResult))
