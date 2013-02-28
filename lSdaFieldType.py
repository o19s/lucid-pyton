import requests
from lSdaCommon import checkRespCode
import sys
import json


class LucidSdaFieldType(object):
    """ Mostly just a convenient wrapper around the
        field type dictionaries that LWE returns"""
    def __init__(self, coll, fieldTypeDict):
        super(LucidSdaFieldType, self).__init__()
        self.collection = coll
        if "name" in fieldTypeDict:
            self.fieldTypeDict = fieldTypeDict
        else:
            raise ValueError("The passed in dictionary does not appear"
                             " to be a LWE field type")

    @staticmethod
    def createWithName(self, coll, name):
        fieldTypeDict = {"name": name}
        return LucidSdaFieldType(coll, fieldTypeDict)

    @property
    def name(self):
        return self.fieldTypeDict["name"]

    @property
    def url(self):
        return self.collection.config.fieldTypesUrl + "/" + \
            self.name

    def create(self):
        config = self.collection.config
        url = config.fieldTypesUrl
        resp = requests.post(url,
                             auth=config.httpAuth,
                             data=json.dumps(self.fieldTypeDict),
                             headers={'Content-type': 'application/json'},
                             config={'verbose': sys.stderr})
        checkRespCode(resp.status_code, self.url, expectedResp=201,
                      errorMsg=resp.text)

    def delete(self):
        config = self.collection.config
        r = requests.delete(self.url,
                            auth=config.httpAuth)
        checkRespCode(r.status_code, self.url)
