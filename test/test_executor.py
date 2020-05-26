
import unittest
from sinbadflow.executor import Sinbadflow
from sinbadflow.pipe import Pipe
from sinbadflow.logger import Logger
from sinbadflow.status_handler import StatusHandler, Status
from unittest import mock
from mock import patch
from collections import namedtuple

RunResults = namedtuple('RunResults', ['status', 'path'])


class ExecutorTest(unittest.TestCase):

    def mock_run(self, path):
        if 'fail' not in path:
            self.run_store[path] = 1
            return RunResults(Status.OK_PREV, path)
        else:
            return RunResults(Status.FAIL_PREV, path)

    def sf_run(self, pipeline):
        with mock.patch.object(self.sf, '_Sinbadflow__run_dbr_notebook', self.mock_run) as _:
            self.sf.run(pipeline)

    def setUp(self):
        self.run_store = {}
        self.sf = Sinbadflow(Logger.EmptyLogger, StatusHandler())

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
        pipeline = Pipe('fail') >> Pipe('ok', Status.FAIL_ALL)
        self.sf_run(pipeline)
        output = {'ok': 1}
        self.assertTrue(self.run_store == output,
                        f"Should get {output}, got: {self.run_store}")

    def test_should_trigger_ok_all(self):
        pipeline = Pipe('ok1') >> Pipe('ok2', Status.OK_ALL)
        self.sf_run(pipeline)
        output = {'ok1': 1, 'ok2': 1}
        self.assertTrue(self.run_store == output,
                        f"Should get {output}, got: {self.run_store}")

    def test_should_trigger_fail_prev(self):
        pipeline = Pipe('fail') >> Pipe('ok', Status.FAIL_PREV)
        self.sf_run(pipeline)
        output = {'ok': 1}
        self.assertTrue(self.run_store == output,
                        f"Should get {output}, got: {self.run_store}")

    def test_should_trigger_ok_prev(self):
        pipeline = Pipe('ok1') >> Pipe('ok2', Status.OK_PREV)
        self.sf_run(pipeline)
        output = {'ok1': 1, 'ok2': 1}
        self.assertTrue(self.run_store == output,
                        f"Should get {output}, got: {self.run_store}")

    def test_should_trigger_done_prev(self):
        pipeline = Pipe('fail') >> Pipe(
            'ok2', Status.OK_PREV) >> Pipe('ok3')
        self.sf_run(pipeline)
        output = {'ok3': 1}
        self.assertTrue(self.run_store == output,
                        f"Should get {output}, got: {self.run_store}")

    def test_should_trigger_works_parallel_notebooks(self):
        pipeline = Pipe('fail') >> [Pipe('ok1', Status.OK_ALL), Pipe(
            'ok2', Status.OK_ALL), Pipe('ok3', Status.FAIL_PREV)]
        self.sf_run(pipeline)
        output = {'ok3': 1}
        self.assertTrue(self.run_store == output,
                        f"Should get {output}, got: {self.run_store}")

    def test_should_trigger_works_at_parallel_notebooks_output(self):
        pipeline = [Pipe('fail'), Pipe('ok2')] >> Pipe('ok3', Status.FAIL_PREV)
        self.sf_run(pipeline)
        output = {'ok2': 1, 'ok3': 1}
        self.assertTrue(self.run_store == output,
                        f"Should get {output}, got: {self.run_store}")

    def test_should_skip_all_after_fail(self):
        pipeline = Pipe('fail') >> Pipe('ok2', Status.OK_PREV) >> Pipe(
            'ok3', Status.OK_ALL) >> [Pipe('fail', Status.OK_PREV), Pipe('ok2', Status.OK_PREV)]
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
        pipeline = Pipe('fail') >> Pipe('ok2', Status.OK_PREV) >> Pipe(
            'ok3', Status.FAIL_PREV) >> [Pipe('fail', Status.OK_PREV), Pipe('ok2', Status.OK_PREV)]
        self.sf_run(pipeline)
        store = self.sf.status_handler.STATUS_STORE
        self.assertTrue(store['SKIPPED'] == 1 and store['FAIL_PREV'] == 2 and store['OK_PREV'] == 2,
                        f"Should get [skipped:1, fail_prev: 2, ok_prev: 2], got {store['SKIPPED'], store['FAIL_PREV'], store['OK_PREV']}")

    def test_should_do_nothing(self):
        pipeline = Pipe() >> Pipe()
        self.sf_run(pipeline)
        self.assertTrue(self.run_store == {},
                        f"Should get nothing, got: {self.run_store}")
