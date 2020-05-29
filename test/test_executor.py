
import unittest
from sinbadflow.executor import Sinbadflow
from sinbadflow.pipe import Pipe
from sinbadflow.utils import Logger, LogLevel
from unittest import mock
from mock import patch
from collections import namedtuple
from sinbadflow.utils import StatusHandler, Trigger, Status

RunResults = namedtuple('RunResults', ['status', 'path'])


class ExecutorTest(unittest.TestCase):

    def mock_run(self, element):
        if not self.sf._Sinbadflow__is_trigger_initiated(element.trigger):
            result_status = Status.SKIPPED
        else:
            if 'fail' not in element.data:
                self.run_store[element.data] = 1
                result_status = Status.OK
            else:
                result_status = Status.FAIL
        return self.sf._Sinbadflow__log_and_return_result(result_status, element)

    def sf_run(self, pipeline):
        with mock.patch.object(self.sf, '_Sinbadflow__execute', self.mock_run) as _:
            self.sf.run(pipeline)

    def setUp(self):
        self.run_store = {}
        self.sh = StatusHandler()
        self.sf = Sinbadflow(Logger.EmptyLogger, self.sh)

    def test_should_run_simple_pipeline(self):
        pipeline = Pipe('ok1') >> Pipe('ok2')
        self.sf_run(pipeline)
        output = {'ok1': 1, 'ok2': 1}
        self.assertTrue(self.run_store == output,
                        f"Should get {output}, got: {self.run_store}")

    def test_should_run_parallel_pipeline(self):
        pipeline = Pipe('ok1') >> [Pipe('ok2'), Pipe('ok3')]
        self.sf_run(pipeline)
        output = {'ok1': 1, 'ok2': 1, 'ok3': 1}
        self.assertTrue(self.run_store == output,
                        f"Should get {output}, got: {self.run_store}")

    def test_should_get_head_from_pipeline(self):
        root = Pipe([Pipe('ok1')])
        pipeline = root >> Pipe('ok2')
        self.sf_run(pipeline)
        self.assertTrue(self.sf.get_head_from_pipeline(pipeline) == root,
                        f"Should get {root}, got: {self.sf.get_head_from_pipeline(pipeline)}")

    def test_should_trigger_fail_all(self):
        pipeline = Pipe('fail') >> Pipe('ok', Trigger.FAIL_ALL)
        self.sf_run(pipeline)
        output = {'ok': 1}
        self.assertTrue(self.run_store == output,
                        f"Should get {output}, got: {self.run_store}")

    def test_should_trigger_ok_all(self):
        pipeline = Pipe('ok1') >> Pipe('ok2', Trigger.OK_ALL)
        self.sf_run(pipeline)
        output = {'ok1': 1, 'ok2': 1}
        self.assertTrue(self.run_store == output,
                        f"Should get {output}, got: {self.run_store}")

    def test_should_trigger_fail_prev(self):
        pipeline = Pipe('fail') >> Pipe('ok', Trigger.FAIL_PREV)
        self.sf_run(pipeline)
        output = {'ok': 1}
        self.assertTrue(self.run_store == output,
                        f"Should get {output}, got: {self.run_store}")

    def test_should_trigger_ok_prev(self):
        pipeline = Pipe('ok1') >> Pipe('ok2', Trigger.OK_PREV)
        self.sf_run(pipeline)
        output = {'ok1': 1, 'ok2': 1}
        self.assertTrue(self.run_store == output,
                        f"Should get {output}, got: {self.run_store}")

    def test_should_trigger_done_prev(self):
        pipeline = Pipe('fail') >> Pipe(
            'ok2', Trigger.OK_PREV) >> Pipe('ok3')
        self.sf_run(pipeline)
        output = {'ok3': 1}
        self.assertTrue(self.run_store == output,
                        f"Should get {output}, got: {self.run_store}")

    def test_should_trigger_works_parallel_notebooks(self):
        pipeline = Pipe('fail') >> [Pipe('ok1', Trigger.OK_ALL), Pipe(
            'ok2', Trigger.OK_ALL), Pipe('ok3', Trigger.FAIL_PREV)]
        self.sf_run(pipeline)
        output = {'ok3': 1}
        self.assertTrue(self.run_store == output,
                        f"Should get {output}, got: {self.run_store}")

    def test_should_trigger_works_at_parallel_notebooks_output(self):
        pipeline = [Pipe('fail'), Pipe('ok2')] >> Pipe('ok3', Trigger.FAIL_PREV)
        self.sf_run(pipeline)
        output = {'ok2': 1, 'ok3': 1}
        self.assertTrue(self.run_store == output,
                        f"Should get {output}, got: {self.run_store}")

    def test_should_skip_all_after_fail(self):
        pipeline = Pipe('fail') >> Pipe('ok2', Trigger.OK_PREV) >> Pipe(
            'ok3', Trigger.OK_ALL) >> [Pipe('fail', Trigger.OK_PREV), Pipe('ok2', Trigger.OK_PREV)]
        self.sf_run(pipeline)
        output = {}
        self.assertTrue(
            self.run_store == output and self.sf.status_handler.STATUS_STORE['SKIPPED'] == 4, f"Should get {output}, got: {self.run_store}")

    def test_should_connect_pipelines_and_run_all(self):
        pipeline = Pipe('ok1') >> Pipe('ok2')
        pipeline2 = Pipe('ok3') >> [Pipe('ok4'), Pipe('ok5')]
        pipeline_sum = pipeline >> pipeline2
        self.sf_run(pipeline)
        output = {'ok1': 1, 'ok2': 1, 'ok3': 1, 'ok4': 1, 'ok5': 1}
        self.assertTrue(self.run_store == output,
                        f"Should get {output}, got: {self.run_store}")

    def test_should_get_correct_statuses(self):
        pipeline = Pipe('fail') >> Pipe('ok2', Trigger.OK_PREV) >> Pipe(
            'ok3', Trigger.FAIL_PREV) >> [Pipe('fail', Trigger.OK_PREV), Pipe('ok2', Trigger.OK_PREV)]
        self.sf_run(pipeline)
        store = self.sf.status_handler.STATUS_STORE
        self.assertTrue(store['SKIPPED'] == 1 and store['FAIL'] == 2 and store['OK'] == 2,
                        f"Should get [skipped:1, fail_prev: 2, ok_prev: 2], got {store['SKIPPED'], store['FAIL'], store['OK']}")

    def test_should_do_nothing(self):
        pipeline = Pipe() >> Pipe()
        self.sf_run(pipeline)
        self.assertTrue(self.run_store == {},
                        f"Should get nothing, got: {self.run_store}")
