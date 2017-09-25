from unittest import TestCase, main
import ericbase

class TestListtodict(TestCase):
    def test_listtodict(self):
        # main(verbosity=2)
        list1 = ["a", "b", "c", "d", "e"]
        list2 = ["first", "second", "third", "fourth", "fifth"]
        testdict = ericbase.listtodict(list1, list2)
        for f in list2:
            self.assertEqual(testdict[f], list1[list2.index(f)])
