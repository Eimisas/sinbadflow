import unittest
from sinbadflow.executor import Sinbadflow
from sinbadflow.utils import StatusHandler, Status, Trigger


class StatusHandlerTest(unittest.TestCase):

    def setUp(self):
        self.sh = StatusHandler()

    def test_should_get_status_ok_all(self):
        self.sh.add_status([Status.OK])
        is_mapped = self.sh.is_status_mapped_to_trigger(Trigger.OK_ALL)
        self.assertTrue(is_mapped,
                        f"Should get True, got: {is_mapped}")

    def test_should_get_status_fail_all(self):
        self.sh.add_status([Status.FAIL])
        is_mapped = self.sh.is_status_mapped_to_trigger(Trigger.FAIL_ALL)
        self.assertTrue(is_mapped,
                        f"Should get True, got: {is_mapped}")

    def test_should_save_and_accumulate_status(self):
        self.sh.add_status([Status.OK, Status.FAIL,
                            Status.OK, Status.OK])
        store = self.sh.STATUS_STORE
        self.assertTrue(self.sh.last_status == Status.FAIL and store['OK'] == 3 and store['FAIL'] == 1,
                        f"Should get last status {Status.FAIL} and 'OK':3, 'FAIL':1 , got: {[self.sh.last_status, store['OK'], store['FAIL']]}")

    def test_should_change_last_status_after_global_all_status(self):
        self.sh.add_status([Status.FAIL, Status.FAIL,
                            Status.FAIL, Status.FAIL])
        self.sh.add_status([Status.OK])
        is_mapped = self.sh.is_status_mapped_to_trigger(Trigger.FAIL_ALL)
        self.assertFalse(is_mapped,
                        f'Should get OK_PREV, and FAIL_ALL map failed, got {is_mapped}')
    
    def test_should_trigger_be_mapped_to_status(self):
        is_ok_mapped_to_ok_all = self.sh.is_status_mapped_to_trigger(Trigger.OK_PREV)
        is_fail_mapped_to_ok_all = self.sh.is_status_mapped_to_trigger(Trigger.FAIL_PREV)
        self.sh.last_status = Status.FAIL_ALL
        is_ok_mapped_to_fail_all = self.sh.is_status_mapped_to_trigger(Trigger.OK_PREV)
        is_fail_mapped_to_fail_all = self.sh.is_status_mapped_to_trigger(Trigger.FAIL_PREV)
        self.sh.last_status = Status.OK
        is_ok_mapped_to_ok = self.sh.is_status_mapped_to_trigger(Trigger.OK_PREV)
        is_fail_mapped_to_ok = self.sh.is_status_mapped_to_trigger(Trigger.FAIL_PREV)
        self.sh.last_status = Status.FAIL
        is_ok_mapped_to_fail = self.sh.is_status_mapped_to_trigger(Trigger.OK_PREV)
        is_fail_mapped_to_fail = self.sh.is_status_mapped_to_trigger(Trigger.FAIL_PREV)
        self.assertTrue(is_ok_mapped_to_ok_all and
                    not is_fail_mapped_to_ok_all and
                    not is_ok_mapped_to_fail_all and
                    is_fail_mapped_to_fail_all and
                    is_ok_mapped_to_ok and
                    not is_fail_mapped_to_ok and
                    not is_ok_mapped_to_fail and
                    is_fail_mapped_to_fail)
