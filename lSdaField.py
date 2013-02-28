#!/usr/bin/env python
import requests
import json
import sys
from lSdaCommon import checkRespCode


class LucidSdaField(object):

    def __init__(self, fieldName, collection,
                 isFacet=False, isMultivalued=False,
                 isStored=True, fieldType="text_en",
                 isIndexed=True, copyFields=[]):
        """ Initialize  the field around a name for the field,
            an instance of a LucidSdaCollection, and
            a config pointing at SDA"""
        self.name = fieldName
        self.collection = collection
        # Set to defaults for the fields as specified
        # in the fields API doc
        self.isFacet = isFacet
        self.isMultivalued = isMultivalued
        self.isStored = isStored
        self.fieldType = fieldType
        self.isIndexed = isIndexed
        self.isDynamic = False
        #may make sense for LW to default this to text_all
        self.copyFields = list(copyFields)
        self.config = self.collection.config

    @staticmethod
    def fromDict(dct, collection):
        """ Factory method From a dict of field params """
        if not type(dct) is dict:
            raise TypeError("LucidSdaField.fromDict requires"
                            " a dictionary with fields instead %s was passed" %
                            str(type(dct)))
        f = LucidSdaField(dct["name"], collection)
        f.isFacet = dct["facet"]
        f.fieldType = dct["field_type"]
        f.isMultiValued = dct["multi_valued"]
        f.isIndexed = dct["indexed"]
        f.copyFields = dct["copy_fields"]
        f.isStored = dct["stored"]
        return f

    def toJson(self):
        """ To JSON understandable by Lucid search REST API"""
        if self.isDynamic == True:
            payload = {"name": self.name,
                       "multi_valued": self.isMultivalued,
                       "field_type": self.fieldType,
                       "indexed": self.isIndexed,
                       "copy_fields": self.copyFields,
                       "stored": self.isStored}
        else:
            payload = {"name": self.name,
                       "facet": self.isFacet,
                       "multi_valued": self.isMultivalued,
                       "field_type": self.fieldType,
                       "indexed": self.isIndexed,
                       "short_field_boost": "moderate",
                       "copy_fields": self.copyFields,
                       "stored": self.isStored}

        if len(payload['copy_fields']) == 0:
            del payload['copy_fields']
        return json.dumps(payload)

    def url(self):
        """ My URL at lucid search """
        return self.config.fieldsUrl + "/" + self.name

    def update(self):
        """ Commit local changes to lucid search API """
        r = requests.put(self.url(),
                         data=self.toJson(),
                         auth=self.config.httpAuth,
                         headers={'content-type': 'application/json'})
        checkRespCode(r.status_code, self.url())

    def delete(self):
        """ Delete this field on the lucidworks API """
        r = requests.delete(self.url(),
                            auth=self.config.httpAuth)
        checkRespCode(r.status_code, self.url())

    def create(self):
        """ Create this field in the lucidworks API """
        if self.isDynamic == True:
            url = self.config.dynamicFieldsUrl
        else:
            url = self.config.fieldsUrl
        r = requests.post(url,
                          auth=self.config.httpAuth,
                          data=self.toJson(),
                          headers={'content-type': 'application/json'},
                          config={'verbose': sys.stderr})
        checkRespCode(r.status_code, self.url(), expectedResp=201,
                      errorMsg=r.text)

    def addCopyField(self, copyField):
        """ Add a field to copy this field too """
        self.copyFields.append(copyField.name)

    def __str__(self):
        return "LucidField: %s" % str(self.__dict__)

def lsFields(fields):
    for field in fields:
        print str(field)
