
import unittest
from sinbadflow.executor import Sinbadflow
from sinbadflow.element import Element
from sinbadflow.utils import Logger, LogLevel
from unittest import mock
from mock import patch
from collections import namedtuple
from sinbadflow.utils import StatusHandler, Trigger, Status, apply_conditional_func
from sinbadflow.agents.base_agent import BaseAgent


class TestAgent(BaseAgent):
        def run(self):
            pass

class ExecutorTest(unittest.TestCase):

    def mock_run(self, element):
        if not self.sf._Sinbadflow__is_trigger_initiated(element.trigger) or not element.conditional_func():
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
        pipeline = TestAgent('ok1') >> TestAgent('ok2')
        self.sf_run(pipeline)
        output = {'ok1': 1, 'ok2': 1}
        self.assertTrue(self.run_store == output,
                        f"Should get {output}, got: {self.run_store}")

    def test_should_run_parallel_pipeline(self):
        pipeline = TestAgent('ok1') >> [TestAgent('ok2'), TestAgent('ok3')]
        self.sf_run(pipeline)
        output = {'ok1': 1, 'ok2': 1, 'ok3': 1}
        self.assertTrue(self.run_store == output,
                        f"Should get {output}, got: {self.run_store}")

    def test_should_get_head_from_pipeline(self):
        root = TestAgent([TestAgent('ok1')])
        pipeline = root >> TestAgent('ok2')
        self.sf_run(pipeline)
        self.assertTrue(self.sf.get_head_from_pipeline(pipeline) == root,
                        f"Should get {root}, got: {self.sf.get_head_from_pipeline(pipeline)}")

    def test_should_trigger_fail_all(self):
        pipeline = TestAgent('fail') >> TestAgent('ok', Trigger.FAIL_ALL)
        self.sf_run(pipeline)
        output = {'ok': 1}
        self.assertTrue(self.run_store == output,
                        f"Should get {output}, got: {self.run_store}")

    def test_should_trigger_ok_all(self):
        pipeline = TestAgent('ok1') >> TestAgent('ok2', Trigger.OK_ALL)
        self.sf_run(pipeline)
        output = {'ok1': 1, 'ok2': 1}
        self.assertTrue(self.run_store == output,
                        f"Should get {output}, got: {self.run_store}")

    def test_should_trigger_fail_prev(self):
        pipeline = TestAgent('fail') >> TestAgent('ok', Trigger.FAIL_PREV)
        self.sf_run(pipeline)
        output = {'ok': 1}
        self.assertTrue(self.run_store == output,
                        f"Should get {output}, got: {self.run_store}")

    def test_should_trigger_ok_prev(self):
        pipeline = TestAgent('ok1') >> TestAgent('ok2', Trigger.OK_PREV)
        self.sf_run(pipeline)
        output = {'ok1': 1, 'ok2': 1}
        self.assertTrue(self.run_store == output,
                        f"Should get {output}, got: {self.run_store}")

    def test_should_trigger_done_prev(self):
        pipeline = TestAgent('fail') >> TestAgent(
            'ok2', Trigger.OK_PREV) >> TestAgent('ok3')
        self.sf_run(pipeline)
        output = {'ok3': 1}
        self.assertTrue(self.run_store == output,
                        f"Should get {output}, got: {self.run_store}")

    def test_should_trigger_works_parallel_notebooks(self):
        pipeline = TestAgent('fail') >> [TestAgent('ok1', Trigger.OK_ALL), TestAgent(
            'ok2', Trigger.OK_ALL), TestAgent('ok3', Trigger.FAIL_PREV)]
        self.sf_run(pipeline)
        output = {'ok3': 1}
        self.assertTrue(self.run_store == output,
                        f"Should get {output}, got: {self.run_store}")

    def test_should_trigger_works_at_parallel_notebooks_output(self):
        pipeline = [TestAgent('fail'), TestAgent('ok2')] >> TestAgent('ok3', Trigger.FAIL_PREV)
        self.sf_run(pipeline)
        output = {'ok2': 1, 'ok3': 1}
        self.assertTrue(self.run_store == output,
                        f"Should get {output}, got: {self.run_store}")

    def test_should_skip_all_after_fail(self):
        pipeline = TestAgent('fail') >> TestAgent('ok2', Trigger.OK_PREV) >> TestAgent(
            'ok3', Trigger.OK_ALL) >> [TestAgent('fail', Trigger.OK_PREV), TestAgent('ok2', Trigger.OK_PREV)]
        self.sf_run(pipeline)
        output = {}
        self.assertTrue(
            self.run_store == output and self.sf.status_handler.STATUS_STORE['SKIPPED'] == 4, f"Should get {output}, got: {self.run_store}")

    def test_should_connect_pipelines_and_run_all(self):
        pipeline = TestAgent('ok1') >> TestAgent('ok2')
        pipeline2 = TestAgent('ok3') >> [TestAgent('ok4'), TestAgent('ok5')]
        pipeline_sum = pipeline >> pipeline2
        self.sf_run(pipeline)
        output = {'ok1': 1, 'ok2': 1, 'ok3': 1, 'ok4': 1, 'ok5': 1}
        self.assertTrue(self.run_store == output,
                        f"Should get {output}, got: {self.run_store}")

    def test_should_get_correct_statuses(self):
        pipeline = TestAgent('fail') >> TestAgent('ok2', Trigger.OK_PREV) >> TestAgent(
            'ok3', Trigger.FAIL_PREV) >> [TestAgent('fail', Trigger.OK_PREV), TestAgent('ok2', Trigger.OK_PREV)]
        self.sf_run(pipeline)
        store = self.sf.status_handler.STATUS_STORE
        self.assertTrue(store['SKIPPED'] == 1 and store['FAIL'] == 2 and store['OK'] == 2,
                        f"Should get [skipped:1, fail_prev: 2, ok_prev: 2], got {store['SKIPPED'], store['FAIL'], store['OK']}")

    def test_should_do_nothing(self):
        pipeline = TestAgent() >> TestAgent()
        self.sf_run(pipeline)
        self.assertTrue(self.run_store == {},
                        f"Should get nothing, got: {self.run_store}")

    def test_should_run_only_one_TestAgent(self):
        pipeline = TestAgent() >> TestAgent('ok')
        self.sf_run(pipeline)
        self.assertTrue(self.run_store == {'ok':1},
                        f"Should run only one element, got: {self.run_store}")                    

    def test_should_run_agent_function(self):
        class DummyAgent(BaseAgent):
            def __init__(self, info):
                self.number = 10
                super(DummyAgent, self).__init__(info)

            def get_inf(self):
                return self.number

            def run(self):
                self.number = self.number * 10

        dummy1 = DummyAgent('one')
        dummy2 = DummyAgent('two')
        pipeline = dummy1 >> dummy2
        self.sf.run(pipeline)
        self.assertTrue(dummy1.number == 100 and dummy2.number == 100, f'Should get dummy numbers as 100, got {dummy1.number, dummy2.number}')

    def test_should_filter_with_conditional_func(self):

        def condi():
            return False
        pipeline = TestAgent('ok1', conditional_func=condi) >> TestAgent('ok2') >> TestAgent('ok3', conditional_func=condi)
        self.sf_run(pipeline)
        self.assertTrue(self.run_store == {'ok2': 1},
                        f"Should run only one element (ok2), got: {self.run_store}")

    def test_should_apply_conditional_func_and_filter(self):
        def condi():
            return False

        pipeline = TestAgent('ok1') >> TestAgent('ok2') >> [
            TestAgent('ok3'), TestAgent('ok4'), TestAgent('ok5')] >> TestAgent('ok6')
        pipeline = apply_conditional_func(pipeline, condi)
        summed = pipeline >> TestAgent('ok7')
        self.sf_run(summed)
        self.assertTrue(self.run_store == {'ok7': 1},
                        f"Should run only one element (ok7), got: {self.run_store}")
