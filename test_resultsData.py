from unittest import TestCase
from getallmessages2 import ResultsData
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


class TestResultsData(TestCase):
    test_r = ResultsData(summary_header)

    def test_newlist(self):
        for k, v in self.test_r.r:
            self.assertEqual(self.test_r.r[k], None)

    def test_keys(self):
        i = 0
        for k in sorted(self.test_r.r):
            self.assertEqual(summary_sort[i], k)
            i += 1

    def test_aslist(self):
        self.fail()

    def test_asdict(self):
        self.fail()

    def test_addlist(self):
        self.fail()
