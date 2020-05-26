from enum import IntEnum
from .logger import Logger, LogLevel
from functools import reduce


class Status(IntEnum):
    '''Sinbadflow status and trigger values 
    ordered to work with logical operators
    '''
    DONE_PREV = 0
    FAIL_ALL = 1
    FAIL_PREV = 2
    OK_PREV = 3
    OK_ALL = 6


class StatusHandler():
    '''StatusHandler class is a part of Sinbadflow used for Status(IntEnum) mapping to triggers, pipe trigger conditions and result storage.

    Methods:
        is_status_mapped_to_trigger(trigger: Status) -> Bool, returns if the trigger is mapped to current last_status variable
        add_status(status: Status), adds status to the STATUS_STORE
        log_results(), logs all results from STATUS_STORE (print is used by default)
    '''

    def __init__(self):
        self.STATUS_STORE = {
            'TOTAL': 0,
            'OK_PREV': 0,
            'FAIL_PREV': 0,
            'SKIPPED': 0
        }
        self.status_to_trigger_map = {
            Status.DONE_PREV: [Status.DONE_PREV, Status.FAIL_ALL, Status.FAIL_PREV, Status.OK_PREV, Status.OK_ALL],
            Status.FAIL_ALL:  [Status.DONE_PREV, Status.FAIL_ALL, Status.FAIL_PREV],
            Status.FAIL_PREV: [Status.DONE_PREV, Status.FAIL_PREV],
            Status.OK_PREV: [Status.DONE_PREV, Status.OK_PREV],
            Status.OK_ALL: [Status.DONE_PREV, Status.OK_ALL, Status.OK_PREV]
        }
        self.status_func_map = {
            Status.DONE_PREV: self.__add_skipped,
            Status.OK_PREV: self.__add_ok,
            Status.FAIL_PREV: self.__add_fail
        }
        self.last_status = Status.OK_ALL

    def is_status_mapped_to_trigger(self, trigger):
        '''Checks if trigger is mapped to current last_status
        Input:
            trigger : Status

        Returns:
            Bool
        '''
        return True if trigger in self.status_to_trigger_map.get(self.last_status) else False

    def add_status(self, result_statuses):
        '''Adds status to the STATUS_STORE. Depending if there are more than

        Input:
            result_statuses: list of Status
        '''
        [self.status_func_map[rs]() for rs in result_statuses]
        # If this is accumulative status and last_status not ALL level
        if len(result_statuses) > 1 and self.last_status not in [Status.OK_ALL, Status.FAIL_ALL]:
            self.last_status = reduce(lambda x, y: x & y, result_statuses)

    def __add_skipped(self):
        self.STATUS_STORE['SKIPPED'] += 1

    def __add_ok(self):
        self.STATUS_STORE['OK_PREV'] += 1
        self.__regenerate_last_status(Status.OK_PREV)

    def __add_fail(self):
        self.STATUS_STORE['FAIL_PREV'] += 1
        self.__regenerate_last_status(Status.FAIL_PREV)

    def __regenerate_last_status(self, status):
        self.STATUS_STORE['TOTAL'] += 1
        if self.STATUS_STORE['TOTAL'] == self.STATUS_STORE[status.name]:
            self.last_status = Status[status.name.replace('PREV', 'ALL')]
            return
        self.last_status = status

    def log_results(self, logger=Logger(print)):
        '''Prints STATUSTORE results'''
        logger.log('\n-----------RESULTS-----------', LogLevel.INFO)
        self.STATUS_STORE['TOTAL'] += self.STATUS_STORE['SKIPPED']
        for key in self.STATUS_STORE:
            logger.log(
                f'{key.replace("_PREV","")} : {self.STATUS_STORE[key]}', LogLevel.INFO)
