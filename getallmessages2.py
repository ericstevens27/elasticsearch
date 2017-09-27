# !/usr/bin/python
# pip install pymongo

import elasticsearch
from optparse import OptionParser
import datetime
import sys
import csv
import re
import json
import ericbase

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
    "type",
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


class ResultsData:
    keys = []
    def __init__(self, kys):
        self.r = {}
        self.keys = kys
        for k in self.keys:
            self.r[k] = None

    def __str__(self):
        return(self.r)

    def __repr__(self):
        return (f'{self.__class__.__name__}('
                f'{self.r!r})')

    def aslist(self):
        return self.r.items()

    def asdict(self):
        return self.r

    def addlist(self, l: list):
        for i in range(0, len(l)):
            self.r[self.keys[i]] = l[i]

class ExtractRe:
    """Class for the regex extraction methods"""

    @staticmethod
    def extractgroup(match):
        """extract the second group (index: 1) from the match object"""
        if match is None:
            return None
        return match.group(1)

    @staticmethod
    def extractgroups(match):
        """extract all of the matching groups from the regex object"""
        if match is None:
            return None
        return match.groups()


class ProcessData:
    """write a list of lists into a csv file"""

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
TO_DATE = "2017-09-25T23:59:00"
DT_FORMAT = '%Y-%m-%dT%H:%M:%S'
HOUR = 3600

DEVICE_MESSAGES = {
    "activationshared": [qry_spaceactivationshared, 'and', qry_DRC, 'and'],
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
    """all of the query related processing and method"""

    def __init__(self):
        self.imei = None
        self.deviceid = None
        self.mailboxid = []
        self.incount = 0
        self.outcount = 0
        self.querystring = ''
        self.queryobject = ''
        self.summary = ResultsData(summary_header)
        self.save = False
        self.results = ResultsData(csv_header)
        self.qrymsg = []
        self.qrytype = ''

    def doquery(self):
        """main query loop for all device messages and mailbox messages for the device id"""
        for self.qrytype, self.qrymsg in DEVICE_MESSAGES.items():
            self.processquery(self.deviceid)
        if self.mailboxid:
            for mb in self.mailboxid:
                for self.qrytype, self.qrymsg in MAILBOX_MESSAGES.items():
                    self.processquery(mb)

    def processquery(self, idtoprocess):
        """form and execute the query, then process the results"""
        self.createquerystring(self.qrymsg, idtoprocess)
        if options.debug:
            print("Created query string of [{}]".format(self.querystring))
        if len(self.querystring) != 0:
            self.createquery()
            if options.verbose:
                print("Processing {}".format(self.querystring))
            if options.debug:
                print(self.queryobject)
            res = {}
            try:
                res = es.search(index=iIndexName, body=self.queryobject)
            except elasticsearch.ConnectionTimeout:
                ericbase.printerror("Connection timed out")
            if options.verbose:
                print("Found {} records".format(res['hits']['total']))
            for hit in res['hits']['hits']:
                self.incount += 1
                self.processmessage(hit["_source"]["message"])
                self.processresults()
                self.processversion()
                if self.save:
                    write_output.writerow(self.results.aslist())
                    self.outcount += 1
                self.summary.r['usefulrecords'] = self.outcount

    def createquery(self):
        """create the query from the string"""
        self.queryobject = esquery_base
        self.queryobject["query"]["bool"]["must"][0]["query_string"]['query'] = self.querystring
        self.queryobject["query"]["bool"]["must"][1]["range"]['@timestamp']['gte'] = FROM_DATE
        self.queryobject["query"]["bool"]["must"][1]["range"]['@timestamp']['lte'] = TO_DATE
        if options.debug:
            print("Check query data: Date from is {}, Date to is {}".format(
                self.queryobject["query"]["bool"]["must"][1]["range"]['@timestamp']['gte'],
                self.queryobject["query"]["bool"]["must"][1]["range"]['@timestamp']['lte']))

    def createquerystring(self, qry_list: list, useid: str):
        """create the correct query string from the list of query items"""
        ql = qry_list + [useid]
        self.querystring = self.quotequery(ql)

    @staticmethod
    def quotequery(qs: list):
        """surround query items with quotes except for AND and OR"""
        qo = []
        for q in qs:
            if q == 'and':
                qo.append(qry_AND)
            elif q == 'or':
                qo.append(qry_OR)
            else:
                qo.append("\"" + q + "\"")
        return ''.join(qo)

    def processversion(self):
        """extract and save the version data"""
        # convert r to dict
        if self.results.r['miuiversion-1'] or self.results.r['miuiversion-2'] or self.results.r['ssversion']:
            n = dict(imei=self.results.r['imei'], datetime=self.results.r['datetime'], date=self.results.r['date'], time=self.results.r['time'])
            if self.results.r['miuiversion-1']:
                n['miuiversion'] = self.results.r['miuiversion-1']
            else:
                n['miuiversion'] = self.results.r['miuiversion-2']
            n['ssversion'] = self.results.r['ssversion']
            n_output = []
            for k, v in n.items():
                n_output.append(v)
            versiondata.outrecords.append(n_output)

    def processresults(self):
        """process the results into the summary record"""
        n = dict(imei=self.summary.r['imei'], fromfile=self.summary.r['fromfile'])
        if self.summary.r['totalmessages'] is None:
            n['totalmessages'] = 1
        else:
            n['totalmessages'] = int(self.summary.r['totalmessages']) + 1
        if self.results.r['subscription']:
            n["subscriptions"] = int(self.summary.r['subscriptions']) + 1
        else:
            if self.summary.r['subscriptions'] is None:
                n['subscriptions'] = 1
            else:
                n['subscriptions'] = int(self.summary.r['subscriptions'])
        if self.summary.r['firstdatetime'] is None:
            n['firstdatetime'] = self.results.r['datetime']
        else:
            if self.results.r['datetime'] < self.summary.r['firstdatetime']:
                n['firstdatetime'] = self.results.r['datetime']
            else:
                n['firstdatetime'] = self.summary.r['firstdatetime']
        if self.summary.r['lastdatetime'] is None:
            n['lastdatetime'] = self.results.r['datetime']
        else:
            if self.results.r['datetime'] > self.summary.r['lastdatetime']:
                n['lastdatetime'] = self.results.r['datetime']
            else:
                n['lastdatetime'] = self.summary.r['lastdatetime']
        if self.results.r['model-1']:
            n['model'] = self.results.r['model-1']
        elif self.results.r['model-2']:
            n['model'] = self.results.r['model-2']
        else:
            n['model'] = self.summary.r['model']
        self.summary.r = n

    def processmessage(self, logmessage: str):
        """process the message using the regex strings to extract the useful data"""
        thisresult = [self.imei, self.querystring, self.qrytype]
        # do datetime separate - need to check for which date group was found
        date_list = matching.extractgroups(re.search(re_datetime, logmessage))
        date_touse = ''
        for dt in date_list:
            if dt is None:
                continue
            else:
                date_touse = dt
        thisresult.append(date_touse)
        [de, t] = matching.extractgroups(re.search(re_datesplit, date_touse))
        thisresult.append(de)
        thisresult.append(t)
        if re_subscription in logmessage:
            thisresult.append(True)
        else:
            thisresult.append(False)
        for re_element in search_list:
            thisresult.append(matching.extractgroup(re.search(re_element, logmessage)))
        if options.includemessages:
            thisresult.append(logmessage)
        if options.debug:
            print(thisresult)
        self.results.addlist(thisresult)

        if thisresult[7] is not None:
            if not self.mailboxid:
                self.mailboxid = [thisresult[7]]
            elif thisresult[7] not in self.mailboxid:
                self.mailboxid.append(thisresult[7])

        self.save = True
        if not options.allrecords:
            # check for useful data indices: 7, 10-18, 20, 21
            self.save = False
            if thisresult[7]:
                self.save = True
            if not self.save:
                for i in range(10, 18):
                    if thisresult[i]:
                        self.save = True
                        break
                if not self.save:
                    for i in range(20, 21):
                        if thisresult[i]:
                            self.save = True
                            break


usagemsg = "This program reads a json file from the current directory and \n\
uses the device ID to extract all messages for this device and associated Spaces from Elasticsearch. \n\
Usage is:\n\n\
python3 " + sys.argv[0] + " [options] where:"

parser = OptionParser(usagemsg)
parser.add_option("-v", "--verbose", dest="verbose", action="store_true", default=False,
                  help="Print out helpful information during processing")
parser.add_option("-d", "--debug", dest="debug", action="store_true", default=False,
                  help="Print out debug messages during processing")
parser.add_option("-t", "--test", dest="test", action="store_true", default=False,
                  help="Use test file instead of full file list")
parser.add_option("-m", "--messages", dest="includemessages", action="store_true", default=False,
                  help="Include full body of message in csv output")
parser.add_option("-a", "--all", dest="allrecords", action="store_true", default=False,
                  help="record all records in csv. Default is only records with useful data.")
parser.add_option("-i", "--input", dest="inputfile", action="store", help="JSON file with list of Device IDs")
(options, args) = parser.parse_args()

# required options checks

rootPath = '.'
d = datetime.datetime.now()
dString = d.strftime("%y%m%d%H%M")
if options.debug:
    options.verbose = True
if options.includemessages:
    csv_header.append('rawmessage')
    if options.verbose:
        print("Appending RAW Messages to message records")
if options.allrecords and options.verbose:
    print("Outputing ALL Message records")

output_file_base = "_messages_" + dString + ".csv"

myESHost = "http://54.223.134.135:9200"
iIndexName = "filebeat*"

if options.test:
    json_files = ["test_devices_list.json"]
    print("[WARNING]: Running in Test Mode")
elif options.inputfile:
    json_files = [options.inputfile]
else:
    json_files = ["normal_devices_list.json"]

summarydata = ProcessData("summary.csv", summary_header)

for f in json_files:
    json_fh = open(f, "r")
    data = json.load(json_fh)
    if options.debug:
        print(data)

    es = elasticsearch.Elasticsearch([myESHost], verify_certs=False, timeout=120)
    matching = ExtractRe()

    if es.indices.exists(iIndexName):
        if options.verbose:
            print('Index there - execute queries')
            print("Using date range of {} to {}".format(FROM_DATE, TO_DATE))

        for record in data:
            if not record['deviceIds']:
                ericbase.printerror("Malformed JSON input file")
            if options.debug:
                for did in record['deviceIds']:
                    print(did)

            for did in record['deviceIds']:
                qryobj = GsQuery()
                qryobj.imei = matching.extractgroup(re.search(re_imei, did))
                qryobj.deviceid = did
                output_file = qryobj.imei + output_file_base
                versiondata = ProcessData(qryobj.imei + "_version.csv", version_header)
                csv_output = open(output_file, 'w')
                write_output = csv.writer(csv_output)
                write_output.writerow(csv_header)
                qryobj.incount = 0
                qryobj.outcount = 0
                qryobj.summary.r['imei'] = qryobj.imei
                qryobj.summary.r['fromfile'] =  f

                qryobj.doquery()

                csv_output.close()
                summarydata.outrecords.append(qryobj.summary.aslist())
                versiondata.outputdata()
                versiondata.outrecords = []
                print("{}: Processed {} records and wrote {} for IMEI [{}]".format(f, qryobj.incount, qryobj.outcount,
                                                                                   qryobj.imei))
    else:
        ericbase.printerror("Indices not found. Check connection.")

summarydata.outputdata()
