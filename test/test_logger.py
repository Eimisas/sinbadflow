import unittest
from sinbadflow.utils import Logger, LogLevel
from unittest.mock import patch, call
import logging
import io
from contextlib import redirect_stdout


class LoggerTest(unittest.TestCase):

    @patch('builtins.print')
    def test_should_print_with_level_info(self, mocked_print):
        lg = Logger(print)
        lg.log('info', LogLevel.INFO)
        self.assertTrue([call('info')] in mocked_print.mock_calls,
                        f'Should call "info", got {mocked_print.mock_calls}')

    @patch('builtins.print')
    def test_should_print_with_level_warning(self, mocked_print):
        lg = Logger(print)
        lg.log('warning', LogLevel.WARNING)
        self.assertTrue([call('warning')] in mocked_print.mock_calls,
                        f'Should call "warning", got {mocked_print.mock_calls}')

    @patch('builtins.print')
    def test_should_print_with_level_critical(self, mocked_print):
        lg = Logger(print)
        lg.log('critical', LogLevel.CRITICAL)
        self.assertTrue([call('critical')] in mocked_print.mock_calls,
                        f'Should call "critical", got {mocked_print.mock_calls}')

    def test_should_log_info(self):
        with self.assertLogs(level='INFO') as log:
            lg = Logger(logging)
            lg.log('info', LogLevel.INFO)
            self.assertIn('info', log.output[0])

    def test_should_log_warning(self):
        with self.assertLogs(level='WARNING') as log:
            lg = Logger(logging)
            lg.log('warning', LogLevel.WARNING)
            self.assertIn('warning', log.output[0])

    def test_should_log_critical(self):
        with self.assertLogs(level='ERROR') as log:
            lg = Logger(logging)
            lg.log('critical', LogLevel.CRITICAL)
            self.assertIn('critical', log.output[0])

    def test_should_not_log_with_empty_logger(self):
        lg = Logger(Logger.EmptyLogger)
        buf = io.StringIO()
        with redirect_stdout(buf):
            lg.log('foobar')
        self.assertNotIn("foobar", buf.getvalue()) 
