from unittest import TestCase, main
import getallmessages2
# from getallmessages2 import ResultsData
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
summary_sort = [
    "firstdatetime",  # 4
    "fromfile",  # 1
    "imei",  # 0
    "lastdatetime",  # 5
    "model",  # 6
    "subscriptions",  # 3
    "totalmessages",  # 2
    "usefulrecords"  # 7
]
listtoadd = [
    '12345678912345',
    'test_file.json',
    25,
    14,
    '2017-08-22 23:08:39,972',
    '2017-08-22 23:08:40,006',
    'Redmi 4A',
    8
]
listtoadd_check = {
    "imei":'12345678912345',  # 0
    "fromfile":'test_file.json', # 1
    "totalmessages": 25, # 2
    "subscriptions":14, # 3
    "firstdatetime":'2017-08-22 23:08:39,972',  # 4
    "lastdatetime":'2017-08-22 23:08:40,006', # 5
    "model":'Redmi 4A',  # 6
    "usefulrecords":8  # 7
}


class TestResultsData(TestCase):
    def test_newlist(self):
        """create a new list"""
        test_r = getallmessages2.ResultsData(summary_header)
        for k in test_r.r:
            self.assertEqual(test_r.r[k], None)

    def test_keys(self):
        """check that I can get all the keys"""
        test_r = getallmessages2.ResultsData(summary_header)
        i = 0
        for k in sorted(test_r.r):
            self.assertEqual(summary_sort[i], k)
            i += 1

    def test_aslist(self):
        """return the results as a list"""
        test_r = getallmessages2.ResultsData(summary_header)
        for el in test_r.aslist():
            self.assertEqual(el, None)

    def test_addlist(self):
        """add a list and check that it is correct"""
        test_r = getallmessages2.ResultsData(summary_header)
        test_r.addlist(listtoadd)
        for k in listtoadd_check:
            self.assertEqual(listtoadd_check[k], test_r.r[k])

