

def getDocs(queryResult):
    try:
        return queryResult['QUERY']['json']['response']['docs']
    except KeyError:
        return []
