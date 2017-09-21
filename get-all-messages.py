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

# regex searches
re_datetime = r"\[DEBUG\] (.*?)\||\[INFO\] (.*?)\||\[ERROR\] (.*?)\|"
re_ssversion = r"\"ssVersion\":\"(.*?)\""
re_mailboxid = r"(SSM-.{8}-.{4}-.{4}-.{4}-.{12})"
re_deviceid = r"(.{8}-.{4}-.{4}-.{4}-.{12}-.{14})"
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
re_subscription = r"Received message via websocket"
re_datesplit = r"^(.*?) (.*?),"
re_imei = r"(\d{15}$)"

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
qry_spaceactiviationindividual = "/activate"
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
    re_forwardedip
]
csv_header = [
    "imei",
    "type",
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
    "forwardedip"
]
summary_header = [
    "imei",  # 0
    "fromfile",  # 1
    "totalmessages",  # 2
    "subscriptions",  # 3
    "firstdatetime",  # 4
    "lastdatetime",  # 5
    "devicerecords",  # 6
    "mailboxrecords",  # 7
    "spacecommandrecords",  # 8
    "devicecommandrecords",  # 9
    "model",  # 10
    "usefulrecords"     # 11
]
summary_blank = [
    0,
    0,
    "",
    "",
    0,
    0,
    0,
    0,
    "",
    0
]
version_header = [
    "imei",  # 0
    "datetime",  # 1
    "date",  # 2
    "time",  # 3
    "miuiversion",  # 4
    "ssversion"  # 5
]
version_blank = [
    "",
    "",
    "",
    "",
    ""
]

record_count = 0
csv_count = 0
summaryrecords = []
versionrecords = []

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


iDteFrom = "2017-08-20T00:00:01.000"
iDteTo = "2017-09-19T23:59:00.000"
re_datecheck = r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}.\d{3}"


def extractgroup(match):
    if match is None:
        return None
    return match.group(1)


def extractgroups(match):
    if match is None:
        return None
    return match.groups()


def processsummarydata():
    summary_file = "summary_aug.csv"
    sf = open(summary_file, "w")
    write_summary = csv.writer(sf)
    write_summary.writerow(summary_header)
    for r in summaryrecords:
        write_summary.writerow(r)
    sf.close()
    return 0


def processversiondata(i):
    version_file = i + "_version_aug.csv"
    sf = open(version_file, "w")
    write_summary = csv.writer(sf)
    write_summary.writerow(version_header)
    for r in versionrecords:
        write_summary.writerow(r)
    sf.close()
    return 0


def listtodict(somelist, keys):
    klen = len(keys)
    if klen != len(somelist):
        print(somelist, keys)
        ericbase.printerror("Key [{}] and list [{}] length mismatch".format(len(keys), len(somelist)))
    dout = {}
    for i in range(0, klen):
        dout[keys[i]] = somelist[i]
    return dout


class gsquery:
    imei = None
    deviceid = None
    mailboxid = None
    incount = 0
    outcount = 0
    querystring = ''
    queryfromdate = "2017-08-20T00:00:01.000"
    querytodate = "2017-09-19T23:59:00.000"
    queryobject = ''
    summary = []
    save = False

    def doquery(self, p):
        if options.debug:
            print("Using class method doquery")
        self.createquerystring("activationshared")
        if options.debug:
            print("Created query string of [{}]".format(self.querystring))
        if len(self.querystring) != 0:
            self.createquery()
            if options.verbose:
                print(
                    "Processing {} [{}]".format(p, self.queryobject["query"]["bool"]["must"][0]["query_string"]['query']))
            if options.debug:
                print(self.queryobject)
            res = es.search(index=iIndexName, body=self.queryobject)
            if options.verbose:
                print("Found {} records".format(res['hits']['total']))
            for hit in res['hits']['hits']:
                self.incount += 1
                self.processmessage(hit["_source"]["message"], p)
                self.processresults()
                self.processversion()
                if self.save:
                    write_output.writerow(self.results)
                    self.outcount += 1
                self.summary.append(self.outcount)
        return 0

    def createquery(self):
        self.queryobject = esquery_base
        self.queryobject["query"]["bool"]["must"][0]["query_string"]['query'] = self.querystring
        self.queryobject["query"]["bool"]["must"][1]["range"]['@timestamp']['gte'] = self.queryfromdate
        self.queryobject["query"]["bool"]["must"][1]["range"]['@timestamp']['lte'] = self.querytodate
        if options.debug:
            print("Check query data: Date from is {}, Date to is {}".format(
                self.queryobject["query"]["bool"]["must"][1]["range"]['@timestamp']['gte'],
                self.queryobject["query"]["bool"]["must"][1]["range"]['@timestamp']['lte']))
        return 0


    def createquerystring(self, qry_type: str):
        querylist = []
        if qry_type == "activationshared":
            querylist = [qry_spaceactivationshared, 'and', qry_DRC, 'and', self.deviceid]
        self.querystring = self.quotequery(querylist)
        return 0


    def quotequery(self, qs: list):
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
        # convert r to dict
        r_dict = listtodict(self.results, csv_header)
        if r_dict['miuiversion-1'] or r_dict['miuiversion-2'] or r_dict['ssversion']:
            n = dict(imei=r_dict['imei'], datetime=r_dict['datetime'], date=r_dict['date'], time=r_dict['time'])
            if r_dict['miuiversion-1']:
                n['miuiversion'] = r_dict['miuiversion-1']
            else:
                n['miuiversion'] = r_dict['miuiversion-2']
            n['ssversion'] = r_dict['ssversion']
            n_output = []
            for k, v in n.items():
                n_output.append(v)
            versionrecords.append(n_output)


    def processresults(self):
        # convert self.results to dict
        r_dict = listtodict(self.results, csv_header)
        # convert self.summary to dict
        s_dict = listtodict(self.summary, summary_header)
        n = dict(imei=s_dict['imei'], fromfile=s_dict['fromfile'])
        n['totalmessages'] = int(s_dict['totalmessages']) + 1
        if r_dict['subscription']:
            n["subscriptions"] = int(s_dict['subscriptions']) + 1
        else:
            n['subscriptions'] = s_dict['subscriptions']
        if r_dict['datetime'] < s_dict['firstdatetime'] or s_dict['firstdatetime'] == '':
            n['firstdatetime'] = r_dict['datetime']
        else:
            n['firstdatetime'] = s_dict['firstdatetime']
        if r_dict['datetime'] > s_dict['lastdatetime'] or s_dict['lastdatetime'] == '':
            n['lastdatetime'] = r_dict['datetime']
        else:
            n['lastdatetime'] = s_dict['lastdatetime']
        type_dict = {
            '[device]': 'devicerecords',
            '[mailbox]': 'mailboxrecords',
            '[spacecommand]': 'spacecommandrecords',
            '[devicecommand]': 'devicecommandrecords'
        }
        for t in type_dict:
            if r_dict['type'] == t:
                n[type_dict[r_dict['type']]] = int(s_dict[type_dict[r_dict['type']]]) + 1
            else:
                n[type_dict[t]] = int(s_dict[type_dict[t]])
        if r_dict['model-1']:
            n['model'] = r_dict['model-1']
        elif r_dict['model-2']:
            n['model'] = r_dict['model-2']
        else:
            n['model'] = s_dict['model']
        n_output = []
        for k, v in n.items():
            n_output.append(v)
        return n_output

    def processmessage(self, logmessage, ptype):
        self.results = [self.imei, ptype, self.querystring]
        # do datetime separate - need to check for which date group was found
        date_list = extractgroups(re.search(re_datetime, logmessage))
        date_touse = ''
        for dt in date_list:
            if dt is None:
                continue
            else:
                date_touse = dt
        self.results.append(date_touse)
        [de, t] = extractgroups(re.search(re_datesplit, date_touse))
        self.results.append(de)
        self.results.append(t)
        if re_subscription in logmessage:
            self.results.append(True)
        else:
            self.results.append(False)
        for re_element in search_list:
            self.results.append(extractgroup(re.search(re_element, logmessage)))
        if options.includemessages:
            self.results.append(logmessage)
        if options.debug:
            print(self.results)
        self.save = True
        if not options.allrecords:
            # check for useful data indices: 7, 10-18, 20, 21
            self.save = False
            if self.results[7]:
                self.save = True
            if not self.save:
                for self.imei in range(10, 18):
                    if self.results[self.imei]:
                        self.save = True
                        break
                if not self.save:
                    for self.imei in range(20, 21):
                        if self.results[self.imei]:
                            self.save = True
                            break


usagemsg = "This program reads a reads a json file from the current directory and \n\
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
else:
    json_files = ["normal_devices_list.json"]

for f in json_files:
    json_fh = open(f, "r")
    data = json.load(json_fh)
    if options.debug:
        print(data)

    es = Elasticsearch([myESHost], verify_certs=False, timeout=120)

    if es.indices.exists(iIndexName):
        if options.verbose:
            print('Index there - execute queries')

        for record in data:
            if options.debug:
                for did in record['deviceIds']:
                    print(did)

            for did in record['deviceIds']:
                # open csv file for this IEMI
                qryobj = gsquery()
                qryobj.imei = extractgroup(re.search(re_imei, did))
                qryobj.deviceid = did
                output_file = qryobj.imei + output_file_base
                csv_output = open(output_file, 'w')
                write_output = csv.writer(csv_output)
                write_output.writerow(csv_header)
                qryobj.incount = 0
                qryobj.outcount = 0
                qryobj.summary = [qryobj.imei, f]
                for elem in summary_blank:
                    qryobj.summary.append(elem)

                # First look for messages related to the deviceId
                qryres = qryobj.doquery('[device]')

            # # Next look for messages related to the mailbox id
            # for el in record['mailboxKeys']:
            #     [summary, record_count, csv_count] = performquery(record['IMEI'], el, '[mailbox]', summary,
            #                                                       record_count, csv_count)
            #
            # # Next look for messages related to the space commands
            # for el in record['spaceCommandKeys']:
            #     [summary, record_count, csv_count] = performquery(record['IMEI'], el, '[spacecommand]', summary,
            #                                                       record_count, csv_count)
            #
            # # Finally look for messages related to the device commands
            # for el in record['deviceCommandKeys']:
            #     [summary, record_count, csv_count] = performquery(record['IMEI'], el, '[devicecommand]', summary,
            #                                                       record_count, csv_count)

                csv_output.close()
                summaryrecords.append(qryobj.summary)
                processversiondata(qryobj.imei)
                versionrecords = []
                print("{}: Processed {} records and wrote {} for IMEI [{}]".format(f, qryobj.incount, qryobj.outcount,
                                                                                   qryobj.imei))
    else:
        ericbase.printerror("Indices not found. Check connection.")

processsummarydata()
