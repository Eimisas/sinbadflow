import unittest
from sinbadflow.executor import Sinbadflow
from sinbadflow.pipe import Pipe
from sinbadflow.status_handler import StatusHandler, Status


class StatusHandlerTest(unittest.TestCase):

    def setUp(self):
        self.sh = StatusHandler()

    def test_should_set_status_ok_all(self):
        self.sh.add_status([Status.OK_PREV])
        self.assertTrue(self.sh.last_status == Status.OK_ALL,
                        f"Should get {Status.OK_ALL}, got: {self.sh.last_status}")

    def test_should_set_status_fail_all(self):
        self.sh.add_status([Status.FAIL_PREV])
        self.assertTrue(self.sh.last_status == Status.FAIL_ALL,
                        f"Should get {Status.FAIL_ALL}, got: {self.sh.last_status}")

    def test_should_save_and_accumulate_status(self):
        self.sh.add_status([Status.OK_PREV, Status.FAIL_PREV,
                            Status.OK_PREV, Status.OK_PREV])
        store = self.sh.STATUS_STORE
        self.assertTrue(self.sh.last_status == Status.FAIL_PREV and store['OK_PREV'] == 3 and store['FAIL_PREV'] == 1,
                        f"Should get last status {Status.FAIL_PREV} and 'OK_PREV':3, 'FAIL_PREV':1 , got: {[self.sh.last_status, store['OK_PREV'], store['FAIL_PREV']]}")

    def test_should_change_last_status_after_global_all_status(self):
        self.sh.add_status([Status.FAIL_PREV, Status.FAIL_PREV,
                            Status.FAIL_PREV, Status.FAIL_PREV])
        tmp_status = self.sh.last_status
        self.sh.add_status([Status.OK_PREV])
        self.assertTrue(tmp_status == Status.FAIL_ALL and self.sh.last_status == Status.OK_PREV,
                        f'Should get FAIL_ALL first and got {tmp_status} and OK_PREV afterwards, got {self.sh.last_status}')
    
    def test_should_trigger_be_mapped_to_status(self):
        is_ok_prev_mapped_to_ok_all = self.sh.is_status_mapped_to_trigger(Status.OK_PREV)
        is_fail_prev_mapped_to_ok_all = self.sh.is_status_mapped_to_trigger(Status.FAIL_PREV)
        self.sh.last_status = Status.FAIL_ALL
        is_ok_prev_mapped_to_fail_all = self.sh.is_status_mapped_to_trigger(Status.OK_PREV)
        is_fail_prev_mapped_to_fail_all = self.sh.is_status_mapped_to_trigger(Status.FAIL_PREV)
        self.sh.last_status = Status.OK_PREV
        is_ok_prev_mapped_to_ok_prev = self.sh.is_status_mapped_to_trigger(Status.OK_PREV)
        is_fail_prev_mapped_to_ok_prev = self.sh.is_status_mapped_to_trigger(Status.FAIL_PREV)
        self.sh.last_status = Status.FAIL_PREV
        is_ok_prev_mapped_to_fail_prev = self.sh.is_status_mapped_to_trigger(Status.OK_PREV)
        is_fail_prev_mapped_to_fail_prev = self.sh.is_status_mapped_to_trigger(Status.FAIL_PREV)
        self.sh.last_status = Status.DONE_PREV
        is_ok_prev_mapped_to_done_prev = self.sh.is_status_mapped_to_trigger(Status.OK_PREV)
        is_fail_prev_mapped_to_done_prev = self.sh.is_status_mapped_to_trigger(Status.FAIL_PREV)
        self.assertTrue(is_ok_prev_mapped_to_ok_all and
                    not is_fail_prev_mapped_to_ok_all and
                    not is_ok_prev_mapped_to_fail_all and
                    is_fail_prev_mapped_to_fail_all and
                    is_ok_prev_mapped_to_ok_prev and
                    not is_fail_prev_mapped_to_ok_prev and
                    not is_ok_prev_mapped_to_fail_prev and
                    is_fail_prev_mapped_to_fail_prev and
                    is_ok_prev_mapped_to_done_prev and
                    is_fail_prev_mapped_to_done_prev)