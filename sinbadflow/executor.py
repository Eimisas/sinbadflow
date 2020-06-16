from .utils import Logger, LogLevel
from .utils import StatusHandler, Status
from concurrent.futures import ThreadPoolExecutor, wait
from .element import Element

class Sinbadflow():
    '''Sinbadflow pipeline runner. Named after famous cartoon "Sinbad: Legend of the Seven Seas" it provides ability to run pipelines made of agents
    with specific triggers and conditional functions in parallel (using ThreadPoolExecutor) or single mode.

    Attributes:
        logging_option = print - selects preferred option of logging (print/logging supported)
        status_handler=StatusHandler() - object used for status to trigger comparison and result retrieval
        log_errors=False - flag to set explicit error logging with preferred logging_option

    Methods:
        run(pipeline: BaseAgent) - runs the input pipeline
        get_head_from_pipeline(pipeline: BaseAgent) -> BaseAgent - returns the head element form the pipeline
        print_pipeline(pipeline: BaseAgent) - logs the full pipeline

    Usage example:

        elem_x = DatabricksAgent('/path/to/notebook', Trigger.OK_PREV)
        elem_y = DatabricksAgent('/path/to/notebook', Trigger.FAIL_PREV)

        pipeline = elem_x >> elem_y
        sf = Sinbadflow()
        sf.run(pipeline)
    '''

    def __init__(self, logging_option=print, status_handler=None, log_errors=False):
        if status_handler:
            self.status_handler = status_handler
        else:
            self.status_handler = StatusHandler()
        self.logger = Logger(logging_option)
        self.log_errors = log_errors
        self.head = None

    def run(self, pipeline):
        '''Runs the input pipeline

        Args:
            pipeline : BaseAgent object

        Example usage:
            pipeline = element1 >> element2
            sinbadflow_instance.run(pipeline)
        '''
        pipeline = self.__wrap_element_if_single(pipeline)
        self.head = self.get_head_from_pipeline(pipeline)
        self.logger.log('Pipeline run started')
        self.__traverse_pipeline(self.__run_elements)
        self.logger.log(f'\nPipeline run finished')
        self.status_handler.print_results(self.logger)

    def __wrap_element_if_single(self, pipeline):
        if type(pipeline) == list:
            return Element(pipeline)
        elif pipeline.prev_elem == None and pipeline.next_elem == None:
            return Element([pipeline])
        return pipeline    

    def get_head_from_pipeline(self, pipeline):
        '''Returns head element from the pipeline

        Args:
            pipeline: BaseAgent object

        Returns:
            BaseAgent (head element)
        '''
        self.__traverse_pipeline(self.__set_head_element, pipeline, False)
        return self.head

    def __traverse_pipeline(self, func, pipeline=None, forward=True):
        pointer = self.head if forward else pipeline
        while pointer is not None:
            func(pointer)
            pointer = pointer.next_elem if forward else pointer.prev_elem

    def __set_head_element(self, elem):
        if elem.prev_elem == None:
            self.head = elem

    def __run_elements(self, elem):
        triggered_elements = self.__get_non_empty_elements_to_execute(elem)
        self.__execute_elements(triggered_elements)

    def __get_non_empty_elements_to_execute(self, element):
        return [elem for elem in element.data if elem.data != None]

    def __execute_elements(self, element_list):
        if not len(element_list):
            return
        self.logger.log('\n-----------PIPELINE STEP-----------')
        self.logger.log(
            f'   Executing pipeline element(s): {[elem.data for elem in element_list]}')
        result_statuses = []
        with ThreadPoolExecutor(max_workers=None) as executor:
            for status in executor.map(self.__execute, element_list):
                result_statuses.append(status)
        self.status_handler.add_status(result_statuses)

    def __execute(self, element):
        if not self.__is_trigger_initiated(element.trigger) or not element.conditional_func():
            result_status = Status.SKIPPED
        else:
            try:
                element.run()
                result_status = Status.OK
            except Exception as e:
                if self.log_errors:
                    self.logger.log(e, LogLevel.CRITICAL)
                result_status = Status.FAIL
        return self.__log_and_return_result(result_status, element)

    def __log_and_return_result(self, status, element):
        if status == Status.SKIPPED:
            conditional_part, func_name = (
                ' or conditional function', f', conditional_func -> {element.conditional_func.__name__}()') if element.conditional_func.__name__ != 'default_func' else ('', '')
            self.logger.log(f'     SKIPPED: Trigger rule{conditional_part} failed for element {element.data}: Element trigger rule -> {element.trigger.name}' +
                            f' and previous run status -> {self.status_handler.last_status.name}{func_name}', LogLevel.WARNING)
            return status
        else:
            level = LogLevel.CRITICAL if status == Status.FAIL else LogLevel.INFO
            self.logger.log(
                f'     Element "{element.data}" run status: {status.name.replace("_PREV","")}', level)
            return status

    def print_pipeline(self, pipeline):
        '''Prints full pipeline

        Args:
            pipeline: BaseAgent
        '''
        self.head = self.get_head_from_pipeline(pipeline)
        self.logger.log(f'↓     -----START-----')
        self.__traverse_pipeline(self.__print_element)
        self.logger.log(f'■     -----END-----')

    def __print_element(self, elem):
        self.logger.log(
            f'''↓     Agent(s) to run: {[("• name: "+type(el).__name__ +
                                        ", data: "+str(el.data)+", trigger: "+
                                        el.trigger.name+", conditional_func: "+
                                        el.conditional_func.__name__+"()") for el in elem.data]}''')

    def __is_trigger_initiated(self, trigger):
        return self.status_handler.is_status_mapped_to_trigger(trigger)
