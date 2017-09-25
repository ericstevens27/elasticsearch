# !/usr/bin/python
# pip install pymongo

from elasticsearch import Elasticsearch
from optparse import OptionParser
import datetime
import sys
import csv
import re
import json
import ericbase
import random
import datetime as dt

# regex searches
re_datetime = r"\[DEBUG\] (.*?)\||\[INFO\] (.*?)\||\[ERROR\] (.*?)\|"
re_ssversion = r"\"ssVersion\":\"(.*?)\""
re_mailboxid = r"(SSM-.{8}-.{4}-.{4}-.{4}-.{12})"
re_deviceid = r"(.{8}-.{4}-.{4}-.{4}-.{12}-.{15})"
re_miuiversion = r"\"fingerprint\":\".*?:.*?/.*?/(.*?):"
re_kernelversionlong = r"\"kernelVersion\":\"(.*?)\""
re_kernelversionshort = r"\"kernelVersion\":\"(.*?)-"
re_platformversion = r"\"platformVersion\":\"(.*?)\""
re_state = r"\"state\":\"(.*?)\""
re_androidversion = r"User-Agent :.*?\(.*?;.*?; (.*?);"
re_model = r"User-Agent.*Android.*; (.*) MIUI"
re_model_2 = r"\"model\":\"(.*?)\""
re_miuiversion_2 = r"User-Agent.*MIUI/(.*?)\)"
re_sequenceid = r"sequenceId\":(\d*?),\"type\":\"subscription"
re_country = r"GEOIP_COUNTRY_NAME : (.*?)\s*GEOIP"
re_city = r"GEOIP_CITY : (.*?)\s*User"
re_realip = r"Device Headers:.*?X-Real-IP : (\d{1,3}.\d{1,3}.\d{1,3}.\d{1,3})"
re_forwardedip = r"Device Headers:.*?X-Forwarded-For : (.*?, .*?) "
re_subscription = r"last subscribed to now"
re_datesplit = r"^(.*?) (.*?),"
re_imei = r"(\d{15}$)"
re_spaceentrycount = r"spaceEntryCount\":(\d*)}"
re_datecheck = r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}.\d{3}"

# query strings
qry_AND = " AND "
qry_OR = " OR "
qry_DRC = "Device Request Content"
qry_deviceregistration_1 = "Device with deviceId"
qry_deviceregistration_2 = "was created"
qry_otadevicedata = "/api/v3/device/devices"
qry_subscription = "last subscribed to now"
qry_lastseendate = "Sent last seen mailbox"
qry_spaceentrycount = "spaceEntryCount"
qry_mailboxregistered = "successfully registered"
qry_recoverspace = "RecoverSpace"
qry_spacedeleted = "deregistered successfully"
qry_devicedeleted = "Removed device"
qry_spaceactivationshared = "/generate"
qry_spaceactivationindividual = "/activate"
qry_spaceactivationarchive = "/lateGenerate"

search_list = [
    re_ssversion,
    re_mailboxid,
    re_deviceid,
    re_miuiversion,
    re_miuiversion_2,
    re_kernelversionshort,
    re_kernelversionlong,
    re_platformversion,
    re_state,
    re_androidversion,
    re_model,
    re_model_2,
    re_sequenceid,
    re_country,
    re_city,
    re_realip,
    re_forwardedip,
    re_spaceentrycount
]
csv_header = [
    "imei",
    "query",
    "datetime",
    "date",
    "time",
    "subscription",
    "ssversion",
    "mailboxid",
    "deviceid",
    "miuiversion-1",
    "miuiversion-2",
    "kernelversionshort",
    "kernelversionlong",
    "platformversion",
    "state",
    "androidversion",
    "model-1",
    "model-2",
    "sequenceid",
    "country",
    "city",
    "realip",
    "forwardedip",
    "spaceentrycount"
]

summary_blank = [
    0,
    0,
    "",
    "",
    "",
    0
]
version_blank = [
    "",
    "",
    "",
    "",
    ""
]

# create the base query
esquery_base = {
    "size": 50000,
    "sort": [
        {
            "@timestamp": {
                "order": "desc",
                "unmapped_type": "boolean"
            }
        }
    ],
    "query": {
        "bool": {
            "must": [
                {
                    "query_string": {
                        "query": "\"59575b7a-b7dc-41e1-8939-c5258436392d\""
                    }
                },
                {
                    "range": {
                        "@timestamp": {
                            "gte": "2017-08-01T00:00:00.000",
                            "lte": "2017-08-30T23:59:00.000"
                        }
                    }
                }
            ],
            "must_not": []
        }
    }
}

summary_header = [
    "imei",  # 0
    "fromfile",  # 1
    "totalmessages",  # 2
    "subscriptions",  # 3
    "firstdatetime",  # 4
    "lastdatetime",  # 5
    "model",  # 6
    "usefulrecords"  # 7
]
version_header = [
    "imei",  # 0
    "datetime",  # 1
    "date",  # 2
    "time",  # 3
    "miuiversion",  # 4
    "ssversion"  # 5
]


class ExtractRe:
    '''Class for the regex extraction methods'''

    @staticmethod
    def extractgroup(match):
        '''extract the second group (index: 1) from the match object'''
        if match is None:
            return None
        return match.group(1)

    @staticmethod
    def extractgroups(match):
        '''extract all of the matching groups from the regex object'''
        if match is None:
            return None
        return match.groups()


class ProcessData:
    '''write a list of lists into a csv file'''

    def __init__(self, fname, h):
        self.outfile = fname
        self.headers = h
        self.outrecords = []

    def outputdata(self):
        sf = open(self.outfile, "w")
        wo = csv.writer(sf)
        wo.writerow(self.headers)
        for r in self.outrecords:
            wo.writerow(r)
        sf.close()
        return 0


FROM_DATE = "2017-08-01T00:00:01"
TO_DATE = "2017-08-15T23:59:00"
DT_FORMAT = '%Y-%m-%dT%H:%M:%S'
HOUR = 3600
DEVICE_MESSAGES = {
    "activationshared": [qry_spaceactivationshared, 'and', qry_DRC],
    "activationindividual": [qry_spaceactivationindividual, 'and', qry_DRC, 'and'],
    "activationarchive": [qry_spaceactivationarchive, 'and', qry_DRC, 'and'],
    "deviceregistration": [qry_deviceregistration_1, 'and', qry_deviceregistration_2, 'and'],
    "devicedata": [qry_otadevicedata, 'and', qry_DRC, 'and'],
    "devicedeleted": [qry_devicedeleted, 'and', qry_DRC, 'and']
}
MAILBOX_MESSAGES = {
    "subscription": [qry_subscription, 'and'],
    "lastseen": [qry_lastseendate, 'and'],
    "spaceentry": [qry_spaceentrycount, 'and'],
    "mailboxregistered": [qry_mailboxregistered, 'and'],
    "recoverspace": [qry_recoverspace, 'and'],
    "spacedeleted": [qry_spacedeleted, 'and']
}


class GsQuery:
    '''all of the query related processing and method'''

    def __init__(self):
        self.incount = 0
        self.outcount = 0
        self.querystring = ''
        self.queryobject = ''
        self.startdate = FROM_DATE
        self.enddate = TO_DATE
        self.results = []

    def processquery(self):
        '''form and execute the query, then process the results'''
        self.createquerystring(DEVICE_MESSAGES['activationshared'])
        if options.debug:
            print("Created query string of [{}]".format(self.querystring))
        if len(self.querystring) != 0:
            self.createquery()
            if options.verbose:
                print("Processing {}".format(self.querystring))
            if options.debug:
                print(self.queryobject)
            res = es.search(index=iIndexName, body=self.queryobject)
            if options.verbose:
                print("Found {} records".format(res['hits']['total']))
            for hit in res['hits']['hits']:
                self.incount += 1
                self.processmessage(hit["_source"]["message"])

    def createquery(self):
        '''create the query from the string'''
        self.queryobject = esquery_base
        self.queryobject["query"]["bool"]["must"][0]["query_string"]['query'] = self.querystring
        self.queryobject["query"]["bool"]["must"][1]["range"]['@timestamp']['gte'] = self.startdate
        self.queryobject["query"]["bool"]["must"][1]["range"]['@timestamp']['lte'] = self.enddate
        if options.debug:
            print("Check query data: Date from is {}, Date to is {}".format(
                self.queryobject["query"]["bool"]["must"][1]["range"]['@timestamp']['gte'],
                self.queryobject["query"]["bool"]["must"][1]["range"]['@timestamp']['lte']))

    def createquerystring(self, qry_list: list):
        '''create the correct query string from the list of query items'''
        ql = qry_list
        self.querystring = self.quotequery(ql)

    @staticmethod
    def quotequery(qs: list):
        '''surround query items with quotes except for AND and OR'''
        qo = []
        for q in qs:
            if q == 'and':
                qo.append(qry_AND)
            elif q == 'or':
                qo.append(qry_OR)
            else:
                qo.append("\"" + q + "\"")
        return ''.join(qo)

    def processmessage(self, logmessage: str):
        '''process the message using the regex strings to extract the useful data'''
        self.results.append(matching.extractgroup(re.search(re_deviceid, logmessage)))


def GetStartEndDates():
    if options.test:
        qry_start_str = "2017-08-01T17:00:00"
        qry_end_str = "2017-08-01T17:01:00"
        if options.debug:
            print("Using TEST query window of {} to {}".format(qry_start_str, qry_end_str))
    else:
        qry_start = random.randint(dt.datetime.timestamp(dt.datetime.strptime(FROM_DATE, DT_FORMAT)),
                                   dt.datetime.timestamp(dt.datetime.strptime(TO_DATE, DT_FORMAT)))
        qry_end = qry_start + HOUR
        qry_start_str = dt.datetime.fromtimestamp(qry_start).strftime(DT_FORMAT)
        qry_end_str = dt.datetime.fromtimestamp(qry_end).strftime(DT_FORMAT)
        if options.debug:
            print("Using time window of: {} to {}".format(FROM_DATE, TO_DATE))
            print("Generated query window of {} to {}".format(qry_start_str, qry_end_str))
    return qry_start_str, qry_end_str


usagemsg = "This program generates a random time period - one hour long, between two dates, then finds all 'generate'\n\
 messages in Elasticsearch during that time window. All found device ids are extracted and saved to a JSON file.\n\
Usage is:\n\n\
python3 " + sys.argv[0] + " [options] where:"

parser = OptionParser(usagemsg)
parser.add_option("-v", "--verbose", dest="verbose", action="store_true", default=False,
                  help="Print out helpful information during processing")
parser.add_option("-d", "--debug", dest="debug", action="store_true", default=False,
                  help="Print out debug messages during processing")
parser.add_option("-t", "--test", dest="test", action="store_true", default=False,
                  help="Use test file instead of full file list")

(options, args) = parser.parse_args()

# required options checks

rootPath = '.'
d = datetime.datetime.now()
dString = d.strftime("%y%m%d%H%M")
if options.debug:
    options.verbose = True

output_file_base = "devices" + dString + ".json"

myESHost = "http://54.223.134.135:9200"
iIndexName = "filebeat*"

if options.test:
    print("[WARNING]: Running in Test Mode")

es = Elasticsearch([myESHost], verify_certs=False, timeout=120)
matching = ExtractRe()

if es.indices.exists(iIndexName):
    if options.verbose:
        print('Index there - execute queries')

    qryobj = GsQuery()
    qryobj.incount = 0
    qryobj.outcount = 0
    (qryobj.startdate, qryobj.enddate) = GetStartEndDates()

    qryobj.processquery()

    print("Processed {} records for date range [{} to {}]".format(qryobj.incount,
                                                                  qryobj.startdate, qryobj.enddate))
    if options.debug:
        print("Devices found:")
        for dev in qryobj.results:
            print(dev)

    outputdict = [dict(deviceIds=qryobj.results)]
    outfh = open(output_file_base, "w")
    outfh.write(json.dumps(outputdict, sort_keys=True, indent=4))
else:
    ericbase.printerror("Indices not found. Check connection.")
