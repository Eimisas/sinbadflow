from enum import IntEnum
from .logger import Logger, LogLevel


class Status(IntEnum):
    '''Sinbadflow run status values'''
    FAIL_ALL = 1
    FAIL = 2
    OK = 3
    OK_ALL = 4
    SKIPPED = 5


class Trigger(IntEnum):
    '''BaseAgent object triggers'''
    DEFAULT = 0
    FAIL_ALL = 1
    FAIL_PREV = 2
    OK_PREV = 3
    OK_ALL = 4


class StatusHandler():
    '''StatusHandler class is a part of Sinbadflow used for status mapping to triggers, determining if element is triggered
    and result storage.

    Methods:
        is_status_mapped_to_trigger(trigger: Status) -> Bool - returns if the trigger is mapped to current last_status variable
        add_status(status: Status) - adds status to the STATUS_STORE, set last_status variable
        print_results() - prints all results from STATUS_STORE
    '''

    def __init__(self):
        self.STATUS_STORE = {
            'OK': 0,
            'FAIL': 0,
            'SKIPPED': 0
        }
        self.status_to_trigger_map = {
            Status.FAIL_ALL:  [Trigger.DEFAULT, Trigger.FAIL_ALL, Trigger.FAIL_PREV],
            Status.FAIL: [Trigger.DEFAULT, Trigger.FAIL_PREV],
            Status.OK: [Trigger.DEFAULT, Trigger.OK_PREV],
            Status.OK_ALL: [Trigger.DEFAULT, Trigger.OK_ALL, Trigger.OK_PREV]
        }
        self.last_status = Status.OK_ALL

    def is_status_mapped_to_trigger(self, trigger):
        '''Checks if trigger is mapped to current last_status

        Args:
            trigger : Status

        Returns:
            Bool
        '''
        if trigger in [Trigger.OK_ALL, Trigger.FAIL_ALL]:
            return self.__is_status_global_all_level(trigger)
        if trigger in self.status_to_trigger_map.get(self.last_status):
            return True
        return False

    def __is_status_global_all_level(self, trigger):
        return self.STATUS_STORE[trigger.name.replace('_ALL', '')] == self.__get_total()

    def add_status(self, result_statuses):
        '''Adds status to the STATUS_STORE.

        Args:
            result_statuses: list of Status
        '''
        for rs in result_statuses:
            self.STATUS_STORE[rs.name] += 1
        self.__set_last_status(result_statuses)

    def __set_last_status(self, result_statuses):
        min_status = min(result_statuses)
        # Change last status if it's not skipped
        self.last_status = min_status if min_status != Status.SKIPPED else self.last_status

    def __get_total(self):
        return self.STATUS_STORE['OK']+self.STATUS_STORE['FAIL']

    def print_results(self, logger=Logger(print)):
        '''Prints STATUSTORE results

        Args:
            logger=Logger(print): Logger object

        '''
        logger.log('\n-----------RESULTS-----------', LogLevel.INFO)
        self.STATUS_STORE['TOTAL'] = self.__get_total() + \
            self.STATUS_STORE['SKIPPED']
        for key in self.STATUS_STORE:
            logger.log(
                f'{key} : {self.STATUS_STORE[key]}', LogLevel.INFO)
