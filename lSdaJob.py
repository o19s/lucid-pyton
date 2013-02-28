import json
import requests
from lSdaCommon import LucidSdaConfiguration, checkRespCode


class LucidSdaJob(object):

    def __init__(self, jobId, config=LucidSdaConfiguration.defaultConf()):
        super(LucidSdaJob, self).__init__()
        self.jobId = jobId
        self.config = config
        self.stat()

    def url(self):
        """ What is my url?"""
        return self.config.jobsUrl + "/" + self.jobId

    def stat(self):
        """ Retreive the latest job status """
        resp = requests.get(url=self.url(),
                            auth=self.config.httpAuth)
        checkRespCode(resp.status_code, self.url())
        status = json.loads(resp.text)
        self.createTime = status["createTime"]
        self.status = status["status"]
        self.workflowId = status["workflowId"]

    def kill(self, promptToVerify=False):
        if promptToVerify:
            print "Are you sure you want to delete job %s?(Y/n)" % \
                self.jobId
            resp = raw_input()
            if resp != "Y":
                print "ABORTED!"
                return
        resp = requests.delete(url=self.url(),
                               auth=self.config.httpAuth)
        checkRespCode(resp.status_code, self.url())

    @staticmethod
    def listJobs(config=LucidSdaConfiguration.defaultConf()):
        """ List all the jobs at the specified config's endpoint"""
        jobLsUrl = config.jobsUrl
        resp = requests.get(jobLsUrl,
                            auth=config.httpAuth)
        checkRespCode(resp.status_code, jobLsUrl)
        return [LucidSdaJob(item['id']) for item in json.loads(resp.text)]


def lsJobs(config=LucidSdaConfiguration.defaultConf()):
    for job in LucidSdaJob.listJobs():
        print " Job:" + job.jobId + " Status:" + job.status
