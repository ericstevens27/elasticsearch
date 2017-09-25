from unittest import TestCase, main
from io import StringIO
from unittest.mock import patch
import ericbase

class TestPrinterror(TestCase):
    @patch('sys.stdout', new_callable=StringIO)
    def test_printerror(self, mock_stdout):
        testmsg = "Test Message"
        expectedmsg = "[ERROR] " + testmsg + "\n\n"
        with patch('ericbase.sys.exit') as exit_mock:
            ericbase.printerror(testmsg)
            self.assertEquals(mock_stdout.getvalue(), expectedmsg)
            assert(exit_mock.called)


