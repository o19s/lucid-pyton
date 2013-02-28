import json


class SearchQuery(object):
    """ Helper class for working with solr queries
    """
    def __init__(self):
        super(SearchQuery, self).__init__()
        self.fieldList = []
        self.qType = "query"  # default SOLR query
        self.queryFields = {}
        self.q = "*:*"

    def addToQueryField(self, fieldName, boost=1.0):
        """ Add something to qf"""
        self.queryFields[fieldName] = boost

    def addToFieldList(self, field):
        """ Add a field to the returned fielid list """
        if not type(field) is str:
            raise ValueError("field should be the name of a field")
        self.fieldList.append(field)

    def __formatFieldList(self):
        if len(self.fieldList) > 0:
            return ",".join(self.fieldList)
        else:
            return "*"

    def __formatQueryFields(self):
        if len(self.queryFields.items()) > 0:
            return " ".join(["%s^%.1lf" % (fieldName, boost)
                             for fieldName, boost
                             in self.queryFields.iteritems()])
        else:
            return "text"

    def asDict(self):
        """ A Json-able dict that once json'd SDA would understand"""
        solr_query = {
            "query": {"q": self.q,
                      "qt": "/lucid",
                      "fl": self.__formatFieldList(),
                      "qf": self.__formatQueryFields(),
                      "rows": 10}}
        return solr_query

    def formatQuery(self):
        return json.dumps(self.asDict(), sort_keys=True, indent=2)
    

class SipsQuery(object):
    def __init__(self, sQuery):
        self.sQuery = sQuery

    def asDict(self):
        solr_query = self.sQuery.asDict()
        sips_query = {"STATISTICALLY_INTERESTING_PHRASES": solr_query["query"]}
        sips_query["STATISTICALLY_INTERESTING_PHRASES"].pop("qt")
        return sips_query

    def formatQuery(self):
        return json.dumps(self.asDict(), sort_keys=True, indent=2)


class SearchResults(object):
    def __init__(self, res, qType):
        super(SearchResults, self).__init__()
        sipsKey = "STATISTICALLY_INTERESTING_PHRASES"
        if "QUERY" in res:
            self.docs = res["QUERY"]["json"]["response"]["docs"]
        elif sipsKey in res:
            self.docs = res[sipsKey]["json"]["response"]["docs"]


def lsResults(res):
    for doc in res.docs:
        print doc['eventname']


def lsRetFields(res):
    for doc in res.docs:
        for key, value in doc.iteritems():
            print "key:%s" % key
            if type(value) is list:
                for i, val in enumerate(value):
                    print val[:20]
                    if i > 20:
                        break
            elif type(value) is str:
                print value[:20]
        print "\n"


class DupQuery(object):
    def __init__(self):
        super(DupQuery, self).__init__()
        self.docId = ""

    def setId(self, docId):
        self.docId = docId

    def formatQuery(self):
        solr_query = {"DUPLICATES": {"id": self.docId}}
        return json.dumps(solr_query)

if __name__ == "__main__":
    import doctest
    doctest.testmod()
