import json
import requests
import sys
from lSdaCommon import LucidSdaConfiguration, checkRespCode, \
    LucidCollConfiguration
from lSdaJob import LucidSdaJob
from lSdaField import LucidSdaField
from lSdaFieldType import LucidSdaFieldType


def embedAwsCredsIntoS3Uri(s3Uri, awsCreds):
    if s3Uri[:5] == "s3://":
        return ("s3://%s:%s@" % (awsCreds.awsAccessKeyId,
                                 awsCreds.awsSecretAccessKey) +
                s3Uri[5:])
    if s3Uri[:6] == "s3n://":
        return ("s3n://%s:%s@" % (awsCreds.awsAccessKeyId,
                                  awsCreds.awsSecretAccessKey) +
                s3Uri[6:])
    return s3Uri


def boolToLowerStr(b):
    if b:
        return "true"
    return "false"


class LucidSdaCollection(object):
    """ This is useful, but is becoming a dumping ground..."""

    def __init__(self, collectionName,
                 config=None,
                 autoCreate=True,
                 trackClicks=False):
        self.config = (LucidCollConfiguration.defaultConf(collectionName)
                       if (config is None) else config)
        super(LucidSdaCollection, self).__init__()
        self.collectionName = collectionName
        self.requestsConfig = {'verbose': sys.stderr}
        self.trackClicks = trackClicks
        self.lastRespToken = 0
        # Validate the collection exists at the URL provided
        if (not self.exists() and autoCreate):
            self.create()
        if not self.exists():
            raise ValueError("Collection: " +
                             collectionName +
                             " does not appear to exist at " +
                             self.config.collectionUrl)

    def create(self):
        """ Trigger the creation of this collection remotely"""
        collectionsUrl = self.config.collectionBaseUrl
        collProperties = {"collection": self.collectionName}
        resp = requests.post(url=collectionsUrl,
                             auth=self.config.httpAuth,
                             data=json.dumps(collProperties),
                             headers={'content-type': 'application/json'},
                             config=self.requestsConfig)
        checkRespCode(resp.status_code, collectionsUrl)
        if not self.exists():
            raise ValueError("Collection: " +
                             self.collectionName +
                             " failed to create at " +
                             self.config.collectionUrl)
        if self.trackClicks:
            self.enableClickTracking()

    def enableClickTracking(self):
        self.trackClicks = True
        if self.trackClicks:
            collSettings = {"click_enabled": "true"}
            resp = requests.put(url=self.config.settingsUrl,
                                auth=self.config.httpAuth,
                                data=json.dumps(collSettings),
                                headers={'content-type': 'application/json'},
                                config=self.requestsConfig)
            checkRespCode(resp.status_code, self.config.settingsUrl,
                          expectedResp=204)

    @staticmethod
    def listCollections(config=LucidSdaConfiguration.defaultConf()):
        collectionsUrl = config.collectionBaseUrl
        requestsConfig = {'verbose': sys.stdout}
        resp = requests.get(url=collectionsUrl,
                            auth=config.httpAuth,
                            config=requestsConfig)
        checkRespCode(resp.status_code, collectionsUrl)
        return json.loads(resp.text)

    def runEtl(self, inputPath, inputMimeType, extractType,
               extraParams={}, doAnalysis=False):
        """ Run the ETL  workflow with stored credentials
            against the specified inputPath and mime type"""
        workflowUrl = self.config.etlWorkflowUrl

        doAnalysisBoolStr = boolToLowerStr(doAnalysis)

        workflowParams = {
                          "collection": self.collectionName,
                          "inputType": inputMimeType,
                          "inputDir": inputPath,
                          "extractType": extractType,
                          "doKMeans": doAnalysisBoolStr,
                          "doSIPs": doAnalysisBoolStr,
                          "doSimdoc": "false",
                          "doAnnotations": "false"}

        workflowParams = dict(workflowParams.items() + extraParams.items())

        print "ABOUT TO POST... %s " % json.dumps(workflowParams)
        resp = requests.post(url=workflowUrl,
                             auth=self.config.httpAuth,
                             data=json.dumps(workflowParams),
                             headers={'content-type': 'application/json'},
                             config=self.requestsConfig)
        checkRespCode(resp.status_code, workflowUrl)
        etlResp = json.loads(resp.text)
        # Return the job (see jobs API) to track
        # progress of the workflow
        job = LucidSdaJob(etlResp['id'])
        return job

    def config(self):
        return self.config

    def exists(self):
        """ return true if the collection exists """
        if (self.stat()['status'] != u"EXISTS"):
            return False
        return True

    def stat(self):
        """ HTTP get the collection directly giving all the info on it"""
        resp = requests.get(url=self.config.collectionUrl,
                            auth=self.config.httpAuth)
        checkRespCode(resp.status_code, self.config.collectionUrl)
        statResp = json.loads(resp.text)
        if statResp['status'] == u"EXISTS":
            self.__refreshFields()
            self.__refreshFieldTypes()
        return json.loads(resp.text)

    def docR(self, solrQ):
        """ Take a query and apply the search to the collection"""
        docRetreivalUri = self.config.docRetrievalUrl
        resp = requests.post(url=docRetreivalUri,
                             auth=self.config.httpAuth,
                             data=json.dumps(solrQ),
                             headers={'content-type': 'application/json'},
                             config=self.requestsConfig)
        print "POST %s" % solrQ
        checkRespCode(resp.status_code, docRetreivalUri)

        if "QUERY" in resp.json:
            self.lastRespToken = \
                resp.json["QUERY"]["json"].get("requestToken", -1)
        return resp.json

    def delete(self, promptToVerify=True):
        """ Delete the collection """
        if promptToVerify:
            print "Are you sure you want to delete collection %s?(Y/n)" % \
                self.collectionName
            resp = raw_input()
            if resp != "Y":
                print "ABORTED!"
                return

        resp = requests.delete(url=self.config.collectionUrl,
                               auth=self.config.httpAuth,
                               config=self.requestsConfig)
        checkRespCode(resp.status_code, self.config.collectionUrl)

    def recordClick(self, docId, pos, timeStamp=None):
        """ Record clicks to lucid's click tracking,
            takes the docId (the id field of the clicked doc) and
            the pos field"""
        import timeUtils
        if self.lastRespToken == -1:
            return

        if timeStamp is None:
            timeStamp = timeUtils.nowInEpochMsecs(),
        queryToRecord = {"type": "c",
                         "req": self.lastRespToken,
                         "doc": docId,
                         "ct": timeStamp,
                         "pos": pos}
        print "PUT... %s" % json.dumps(queryToRecord)
        resp = requests.put(url=self.config.clickTrackingUrl,
                            auth=self.config.httpAuth,
                            config=self.requestsConfig,
                            data=json.dumps(queryToRecord),
                            headers={'content-type': 'application/json'})
        checkRespCode(resp.status_code, self.config.clickTrackingUrl)

    def getAnalysis(self, metricName, startDate, endDate):
        """ Return the raw analysis figures"""
        startDateStr = startDate.strftime("%Y-%m-%d")
        endDateStr = endDate.strftime("%Y-%m-%d")

        analysisQuery = {"metrics":
                         {metricName:
                         {"startDate": startDateStr,
                          "endDate": endDateStr,
                          "aggregrates": [],
                          "includeSeries": "true",
                          "metricName": metricName}}}

        print "POST... %s" % json.dumps(analysisQuery)
        resp = requests.post(url=self.config.analysisUrl,
                             auth=self.config.httpAuth,
                             config=self.requestsConfig,
                             data=json.dumps(analysisQuery),
                             headers={'content-type': 'application/json'})
        checkRespCode(resp.status_code, self.config.analysisUrl)
        return resp.json

    def __refreshFields(self):
        """ Refresh the fields in the collection """
        resp = requests.get(url=self.config.fieldsUrl,
                            auth=self.config.httpAuth,
                            config=self.requestsConfig)
        checkRespCode(resp.status_code, self.config.fieldsUrl)
        fields = json.loads(resp.text)
        self.fields = [LucidSdaField.fromDict(field, self) for field in fields]

    def __refreshFieldTypes(self):
        """ Refresh the field types in the collection """
        resp = requests.get(url=self.config.fieldTypesUrl,
                            auth=self.config.httpAuth,
                            config=self.requestsConfig)
        checkRespCode(resp.status_code, self.config.fieldTypesUrl)
        self.fieldTypes = [LucidSdaFieldType(self, fieldType) for fieldType in resp.json]


def ls():
    for coll in LucidSdaCollection.listCollections():
        print coll["collection"]

if __name__ == "__main__":
    import doctest
    doctest.testmod()
