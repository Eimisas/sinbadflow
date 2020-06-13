from sinbadflow.utils.dbr_job import *
from unittest.mock import patch, call, Mock
from unittest import mock
import unittest


class DummyRequests():
    def post(path, json, headers=None):
        mock_resp = Mock()
        path = json.get('notebook_task').get('notebook_path')

        path_values = {
            'ok': 1,
            'fail': 2,
            'timeout': 3,
            'noToken': 2,
            'skipped': 4,
            'canceled': 5,
            'error': 6
        }
        mock_resp.json = Mock(
            return_value={'run_id': path_values.get(path)})

        return mock_resp

    def get(path, headers=None, flg=None):

        value = path.partition('?')[2]
        result_state_values = {
            'run_id=1': 'SUCCESS',
            'run_id=2': 'FAILED',
            'run_id=3': 'TIMEDOUT',
            'run_id=4': None,
            'run_id=5': 'CANCELED',
            'run_id=6': 'FAILED'
        }
        life_cycle_state_values = {
            'run_id=1': 'TERMINATED',
            'run_id=2': 'TERMINATED',
            'run_id=3': 'TERMINATED',
            'run_id=4': 'SKIPPED',
            'run_id=5': 'TERMINATED',
            'run_id=6': 'INTERNAL_ERROR'
        }
        mock_resp = Mock()
        mock_resp.json = Mock(
            return_value={'job_id': 1000,
                          'run_id': 1000,
                          'state': {'life_cycle_state': life_cycle_state_values.get(value),
                                    'result_state': result_state_values.get(value),
                                    'state_message': ''},
                          'task': {'notebook_task': {'notebook_path': 'path/to/notebook/fail'}},
                          'run_page_url': 'https://westeurope.azuredatabricks.net/12121212#job/999/run/1'}
        )
        return mock_resp


@mock.patch('requests.get', side_effect=DummyRequests.get)
@mock.patch('requests.post', side_effect=DummyRequests.post)
class JobSubmittTest(unittest.TestCase):

    def setUp(self):
        self.default_job_args = {"spark_version": "6.4.x-scala2.11",
                                 "node_type_id": "Standard_DS3_v2",
                                 "num_workers": 2}
        self.js = JobSubmitter('job', self.default_job_args)

    def test_should_run_ok(self, mock_get, mock_post):
        self.js.set_access_token('tokentokentoken')
        result = self.js.submit_notebook('ok', 5, {})
        output = None
        self.assertTrue(result == output,
                        f"Should get {output}, got: {result}")

    def test_should_run_fail(self, mock_get, mock_post):
        self.js.set_access_token('tokentokentoken')
        self.assertRaises(
            RunStatusError, lambda: self.js.submit_notebook('fail', 5, {}))

    def test_should_fail_no_token(self, mock_get, mock_post):
        self.js.set_access_token(None)
        self.assertRaises(
            NoTokenError, lambda: self.js.submit_notebook('noToken', 5, {}))

    def test_should_fail_on_skipped_status(self, mock_get, mock_post):
        self.js.set_access_token('tokentokentoken')
        self.assertRaises(
            RunStatusError, lambda: self.js.submit_notebook('skipped', 5, {}))

    def test_should_fail_on_canceled_status(self, mock_get, mock_post):
        self.js.set_access_token('tokentokentoken')
        self.assertRaises(
            RunStatusError, lambda: self.js.submit_notebook('canceled', 5, {}))

    def test_should_fail_on_internal_error(self, mock_get, mock_post):
        self.js.set_access_token('tokentokentoken')
        self.assertRaises(
            RunStatusError, lambda: self.js.submit_notebook('error', 5, {}))