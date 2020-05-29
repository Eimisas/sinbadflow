from .utils import Logger, LogLevel
from .utils import StatusHandler, Status, Trigger
from concurrent.futures import ThreadPoolExecutor, wait
from collections import namedtuple

RunResults = namedtuple('RunResults', ['status', 'path'])


class Sinbadflow():
    '''Sinbadflow pipeline runner. Named after famous cartoon "Sinbad: Legend of the Seven Seas" it provides ability to run pipelines with databricks notebooks
    and specific triggers in parallel or single mode. The main component of Sinbadflow - Pipe() object from which pipelines are build.

    Initialize options:
    logging_option = print - selects prefered option of logging (print/logging supported)

    Methods:
    run(pipeline: Pipe, timeout=1800) - runs the input pipeline with specific dbutils.notebook.run timeout parameter
    get_head_from_pipeline(pipeline: Pipe) -> Pipe(), returns the head pipe form the pipeline
    print_pipeline(pipeline: Pipe) - prints the full pipeline

    Usage example:
    pipe_x = Pipe('/path/to/notebook', Trigger.OK_PREV)
    pipe_y = Pipe('/path/to/notebook', Trigger.FAIL_PREV)

    pipeline = pipe_x >> pipe_y
    sf = Sinbadflow()
    sf.run(pipeline)
    '''

    def __init__(self, logging_option=print, status_handler=StatusHandler()):
        self.status_handler = status_handler
        self.logger = Logger(logging_option)
        self.head = None

    def run(self, pipeline, timeout=1800):
        '''Runs the input pipeline with specific timeout (dbutils.notebook.run parameter)
        Inputs:
        pipeline : Pipe object
        timeout: int, default = 1800

        Example usage:
        pipeline = pipe1 >> pipe2
        sinbadflow_instance.run(pipeline, 1000)
        '''
        self.head = self.get_head_from_pipeline(pipeline)
        self.logger.log('Pipeline run started')
        self.timeout = timeout
        self.__traverse_pipeline(self.__run_notebooks)
        self.logger.log(f'\nPipeline run finished')
        self.status_handler.print_results(self.logger)

    def get_head_from_pipeline(self, pipeline):
        '''Returns head pipe from the pipeline
        Inputs:
        pipeline: Pipe object

        Returns:
        Pipe (head pipe)
        '''
        self.__traverse_pipeline(self.__set_head_pipe, pipeline, False)
        return self.head

    def __traverse_pipeline(self, func, pipeline=None, forward=True):
        pointer = self.head if forward else pipeline
        while pointer is not None:
            func(pointer)
            pointer = pointer.next_pipe if forward else pointer.prev_pipe

    def __set_head_pipe(self, pipe):
        if pipe.prev_pipe == None:
            self.head = pipe

    def __run_notebooks(self, elem):
        self.logger.log('\n-----------PIPELINE STEP-----------')
        triggered_notebooks = self.__get_notebooks_to_execute(elem)
        self.__execute_elements(triggered_notebooks)

    def __get_notebooks_to_execute(self, elem):
        triggered_notebooks = []
        for pipe in elem.data:
            if pipe.data == None:
                continue
            triggered_notebooks.append(pipe)
        return triggered_notebooks

    def __execute_elements(self, element_list):
        if not len(element_list):
            return
        self.logger.log(
            f'   Executing pipeline element(s): {[elem.data for elem in element_list]}')
        result_statuses = []
        with ThreadPoolExecutor(max_workers=None) as executor:
            for status in executor.map(self.__execute, element_list):
                result_statuses.append(status)
        self.status_handler.add_status(result_statuses)

    def __execute(self, element):
        if not self.__is_trigger_initiated(element.trigger):
            result_status = Status.SKIPPED
        else:
            try:
                ##TODO should execute - self.run()
                dbutils.notebook.run(element.data, self.timeout)
                result_status = Status.OK
            except Exception as e:
                result_status = Status.FAIL
        return self.__log_and_return_result(result_status, element)

    def __log_and_return_result(self, status, element):
        if status == Status.SKIPPED:
            self.logger.log(f'     SKIPPED: Trigger rule failed for notebook {element.data}: Pipe trigger rule -> {element.trigger.name}' +
                            f' and previous run status -> {self.status_handler.last_status.name}', LogLevel.WARNING)
            return status
        else:
            level = LogLevel.CRITICAL if status == Status.FAIL else LogLevel.INFO
            self.logger.log(
                f'     Notebook {element.data} run status: {status.name.replace("_PREV","")}', level)
            return status

    def print_pipeline(self, pipeline):
        '''Prints full visual pipeline'''
        self.head = self.get_head_from_pipeline(pipeline)
        self.logger.log(f'↓     -----START-----')
        self.__traverse_pipeline(self.__print_element)
        self.logger.log(f'■     -----END-----')

    def __print_element(self, elem):
        self.logger.log(
            f'↓     Element {elem} with agents and triggers: {[(elem.data, elem.trigger.name) for elem in elem.data]}')

    def __is_trigger_initiated(self, trigger):
        return self.status_handler.is_status_mapped_to_trigger(trigger)
