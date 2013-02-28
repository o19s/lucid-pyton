from lSdaCollection import LucidSdaCollection

import random
from sys import argv
import time
import timeUtils

random.seed(int(round(time.time())))

extraTerms = ["blog", "facebook", "cow", "hat", "monitor", "cashew",
              "nuts", "backpack", "book", "bottle", "company",
              "open", "source", "fan", "paint", "syringe", "needle"]


def clickDocs(docs):
    firstDoc = docs[0]
    topics = firstDoc["annotations_topic_value"]
    for (pos, doc) in enumerate(docs):
        timeStamp = \
            timeUtils.randomTimestampIsoDate("2012-01-01", "2012-11-13")
        documentId = doc["documentid"][0]
        clickHere = random.choice(["Click", "Skip", "Click", "Skip",
                                   "Click", "Skip", "Quit"])
        if clickHere == "Click":
            print "Clicking doc %i, id: %s at %i" % (pos,
                                                     documentId,
                                                     timeStamp / 1000)
            coll.recordClick(documentId, pos, timeStamp=timeStamp)
        if clickHere == "Skip":
            print "Not clicking %i, id: %s" % (pos, documentId)
        if clickHere == "Quit":
            print "Done"
            break
    return random.choice(topics) + " " + random.choice(extraTerms)


def doClicks(coll, term):
    qResp = coll.docR({"query": {"q": term, "rows": 10, "qt": "/lucid"}})
    if "QUERY" in qResp:
        docs = [doc for
                doc in
                qResp["QUERY"]["json"]["response"]["docs"]]
        if len(docs) > 0:
            return clickDocs(docs)
    return random.choice(extraTerms)

coll = LucidSdaCollection("bright-planet-5")
term = argv[1]
while True:
    print "Searching... %s" % term
    term = doClicks(coll, term)
