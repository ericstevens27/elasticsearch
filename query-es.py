# !/usr/bin/python
# pip install pymongo

from elasticsearch import Elasticsearch
from optparse import OptionParser
import datetime
import sys
import csv
import re
import json

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

iDteFrom = "2017-09-05T16:55:00.000"
iDteTo = "2017-09-05T17:00:00.000"
re_datecheck = r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}.\d{3}"


def extractgroup(match):
    if match is None:
        return None
    return match.group(1)


def extractgroups(match):
    if match is None:
        return None
    return match.groups()


def printerror(errmsg):
    print("[ERROR] {}\n".format(errmsg))
    sys.exit(2)


def processmessage(logmessage, i, ptype, qid):
    sr = [i, ptype, qid]
    # do datetime separate - need to check for which date group was found
    date_list = extractgroups(re.search(re_datetime, logmessage))
    date_touse = ''
    for dt in date_list:
        if dt is None:
            continue
        else:
            date_touse = dt
    sr.append(date_touse)
    [de, t] = extractgroups(re.search(re_datesplit, date_touse))
    sr.append(de)
    sr.append(t)
    if re_subscription in logmessage:
        sr.append(True)
    else:
        sr.append(False)
    for re_element in search_list:
        sr.append(extractgroup(re.search(re_element, logmessage)))
    if options.includemessages:
        sr.append(logmessage)
    if options.debug:
        print(sr)
    savethisrecord = True
    if not options.allrecords:
        # check for useful data indices: 7, 10-18, 20, 21
        savethisrecord = False
        if sr[7]:
            savethisrecord = True
        if not savethisrecord:
            for i in range(10, 18):
                if sr[i]:
                    savethisrecord = True
                    break
            if not savethisrecord:
                for i in range(20, 21):
                    if sr[i]:
                        savethisrecord = True
                        break
    return savethisrecord, sr


def createquery(qid, frm, to):
    q = esquery_base
    q["query"]["bool"]["must"][0]["query_string"]['query'] = "\"" + qid + "\""
    q["query"]["bool"]["must"][1]["range"]['@timestamp']['gte'] = frm
    q["query"]["bool"]["must"][1]["range"]['@timestamp']['lte'] = to
    if options.debug:
        print("Check query data: Date from is {}, Date to is {}".format(
            q["query"]["bool"]["must"][1]["range"]['@timestamp']['gte'],
            q["query"]["bool"]["must"][1]["range"]['@timestamp']['lte']))
    return q


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
        printerror("Key [{}] and list [{}] length mismatch".format(len(keys), len(somelist)))
    dout = {}
    for i in range(0, klen):
        dout[keys[i]] = somelist[i]
    return dout


def processresults(r, s):
    # convert r to dict
    r_dict = listtodict(r, csv_header)
    # convert s to dict
    s_dict = listtodict(s, summary_header)
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


def processversion(r):
    # convert r to dict
    r_dict = listtodict(r, csv_header)
    n = {}
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
        return n_output
    else:
        return None


def printoutput(r):
    i=0
    print ("\n---- {}: {} ----".format(csv_header[i+2], r[i+2]))
    i += 3
    print ("{}: {}\t{}: {}\t{}: {}".format(csv_header[i], r[i], csv_header[i+1], r[i+1], csv_header[i+2], r[i+2]))
    i += 3
    print ("{}: {}\t{}: {}\t{}: {}".format(csv_header[i], r[i], csv_header[i+2], r[i+2], csv_header[i+3], r[i+3]))
    i += 4
    print ("{}: {}".format(csv_header[7], r[7]))
    print ("{}: {}\t{}: {}".format(csv_header[i], r[i], csv_header[i+1], r[i+1]))
    i += 2
    print ("{}: {}\t{}: {}".format(csv_header[i], r[i], csv_header[i+1], r[i+1]))
    i += 2
    print ("{}: {}\t{}: {}".format(csv_header[i], r[i], csv_header[i+2], r[i+2]))
    i += 3
    print ("{}: {}\t{}: {}".format(csv_header[15], r[15], csv_header[19], r[19]))
    print ("{}: {}\t{}: {}".format(csv_header[i], r[i], csv_header[i+1], r[i+1]))
    i += 3
    print ("{}: {}\t{}: {}\t{}: {}\t{}: {}".format(csv_header[i], r[i], csv_header[i+1], r[i+1], csv_header[i+2], r[i+2], csv_header[i+3], r[i+3]))
    return 0


def performquery(i, q, p, s, r_count, c_count):
    esquery = createquery(q, iDteFrom, iDteTo)
    if options.verbose:
        print(
            "Processing {} [{}]".format(p, esquery["query"]["bool"]["must"][0]["query_string"]['query']))
    res = es.search(index=iIndexName, body=esquery)
    if options.verbose:
        print("Found {} records".format(res['hits']['total']))
    for hit in res['hits']['hits']:
        r_count += 1
        (save, results) = processmessage(hit["_source"]["message"], i, p, q)
        s = processresults(results, s)
        v = processversion(results)
        if v is not None:
            versionrecords.append(processversion(results))
        if save:
            if options.csvout:
                write_output.writerow(results)
            else:
                printoutput(results)
            c_count += 1
        s.append(c_count)
    return s, r_count, c_count


usagemsg = "This program reads a reads a json file from the current directory and \n\
uses the device ID, Mailbox ID and Command Keys to extract messages from Elasticsearch. \n\
Usage is:\n\
python3 " + sys.argv[0] + " [options] where options can be:"

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
parser.add_option("-c", "--csv", dest="csvout", action="store_true", default=False,
                  help="record all records in csv. Default is only records with useful data.")
parser.add_option("-q", "--query", dest="query", action="store", help="The query to use in the Elasticsearch call")
parser.add_option("-s", "--startdate", dest="startdate", action="store", help="The start date to use in format: YYYY-MM-DDTHH:MM:SS.mmm")
parser.add_option("-e", "--enddate", dest="enddate", action="store", help="The start date to use in format: YYYY-MM-DDTHH:MM:SS.mmm")

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
        print ("Appending RAW Messages to message records")
if options.allrecords and options.verbose:
    print ("Outputing ALL Message records")
if options.csvout:
    print ("Output records to CSV file")
else:
    print ("printing output to STDOUT")
# check dates
if options.startdate:
    if re.match(re_datecheck, options.startdate):
        iDteFrom = options.startdate
    else:
        printerror("Start date is not of the correct format. Must be YYYY-MM-DDTHH:MM:SS.mmm")
else:
    if options.verbose:
        print ("Using start date of {}".format(iDteFrom))
if options.enddate:
    if re.match(re_datecheck, options.enddate):
        iDteTo = options.enddate
    else:
        printerror("End date is not of the correct format. Must be YYYY-MM-DDTHH:MM:SS.mmm")
else:
    print ("Using end date of {}".format(iDteTo))

output_file_base = "_messages.csv"

myESHost = "http://54.223.134.135:9200"
iIndexName = "filebeat*"

es = Elasticsearch([myESHost], verify_certs=False, timeout=120)

if es.indices.exists(iIndexName):
    if options.verbose:
        print('Index there - execute queries')

    # open csv file for this query
    output_file = options.query + output_file_base
    csv_output = open(output_file, 'w')
    write_output = csv.writer(csv_output)
    write_output.writerow(csv_header)
    record_count = 0
    csv_count = 0
    summary = [options.query, 'manual']
    for elem in summary_blank:
        summary.append(elem)

    # First look for messages related to the deviceId
    [summary, record_count, csv_count] = performquery(options.query, options.query, '[manual]', summary,
                                                      record_count, csv_count)

    csv_output.close()
    summaryrecords.append(summary)
    processversiondata(options.query)
    versionrecords = []
    print("Processed {} records and wrote {} for query [{}]".format(record_count, csv_count,
                                                                       options.query))
else:
    printerror("Indices not found. Check connection.")

processsummarydata()
