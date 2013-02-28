from lSdaCollection import LucidSdaCollection
import json

coll = LucidSdaCollection("bright-planet-5")
print json.dumps(coll.docR({"query":
                           {"group": True,
                            "group.field": "clusterId",
                            "group.limit": 2,
                            "q": "text:*",
                            "rows": 2}}))
