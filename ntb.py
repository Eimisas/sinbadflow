# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ### Sinbadflow
# MAGIC
# MAGIC Simple pipeline tool for workflow management, purely based on Databricks

# COMMAND ----------

from forbiddenfruit import curse
from functools import reduce
import logging
from collections import namedtuple
from concurrent.futures import ThreadPoolExecutor, wait
from enum import IntEnum, Enum
from enum import Enum
dbutils.library.installPyPI('forbiddenfruit')

# COMMAND ----------

# MAGIC %md
# MAGIC Logger

# COMMAND ----------


class Logger():
  '''Logger used in Sinbadflow pipeline builder. Currently 'print', 'logging' and inner class 'EmptyLogger' functionality is supported.
  Initialize options:
    method = selects prefered option of logging (print/logging objects supported)
  
  Methods:
    log(message: string, level=Level.INFO: internal Level enum)
  
  Objects:
    class LogLevel(Enum) - used to select specific log level
    class EmptyLogger - logger object with 'log' method used for testing to keep stdout empty
  
  Usage example:
    lg = Logger(logging)
    lg.log('test', Logger.LogLevel.WARNING) ---> logging.warning('test')
  '''
  class LogLevel(Enum):
    INFO = 0
    WARNING = 1
    CRITICAL = 2

  class EmptyLogger():
    def log(message):
      pass

  def __init__(self, method):
    self.method = method
    self.level_to_method = {
        print: {
            Logger.LogLevel.INFO: print,
            Logger.LogLevel.WARNING: print,
            Logger.LogLevel.CRITICAL: print
        },
        logging: {
            Logger.LogLevel.INFO: logging.info,
            Logger.LogLevel.WARNING: logging.warning,
            Logger.LogLevel.CRITICAL: logging.error
        },
        Logger.EmptyLogger: {
            Logger.LogLevel.INFO: Logger.EmptyLogger.log,
            Logger.LogLevel.WARNING: Logger.EmptyLogger.log,
            Logger.LogLevel.CRITICAL: Logger.EmptyLogger.log
        }
    }

  def log(self, message, level=LogLevel.INFO):
    self.level_to_method[self.method][level](message)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Global variables and StatusHandler

# COMMAND ----------


'''Named tuples used for result output'''
RunResults = namedtuple('RunResults', ['status', 'path'])


class Status(IntEnum):
  '''Sinbadflow run status values'''
  FAIL_ALL = 1
  FAIL = 2
  OK = 3
  OK_ALL = 4
  SKIPPED = 5


class Trigger(IntEnum):
  '''Pipe object triggers'''
  DEFAULT = 0
  FAIL_ALL = 1
  FAIL_PREV = 2
  OK_PREV = 3
  OK_ALL = 4


class StatusHandler():
  '''StatusHandler class is a part of Sinbadflow used for status mapping to triggers, determining if pipe is triggered
  and result storage.
  
  Methods:
    is_status_mapped_to_trigger(trigger: Status) -> Bool, returns if the trigger is mapped to current last_status variable
    add_status(status: Status), adds status to the STATUS_STORE, set last_status variable
    print_results(), prints all results from STATUS_STORE
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
    self.status_func_map = {
        Status.SKIPPED: self.__add_skipped,
        Status.OK: self.__add_ok,
        Status.FAIL: self.__add_fail
    }
    self.last_status = Status.OK_ALL

  def is_status_mapped_to_trigger(self, trigger):
    '''Checks if trigger is mapped to current last_status
    Input:
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
    return True if self.STATUS_STORE[trigger.name.replace('_ALL', '')] == self.__get_total() else False

  def add_status(self, result_statuses):
    '''Adds status to the STATUS_STORE. Depending if there are more than one status passed
    and last status is not FAIL_ALL or OK_ALL, the last_status is accumulated using reduce
    function with logical & operator.

    Input:
    result_statuses: list of Status
    '''
    for rs in result_statuses:
      self.status_func_map[rs]()
    self.__set_last_status(result_statuses)

  def __add_skipped(self):
    self.STATUS_STORE['SKIPPED'] += 1

  def __add_ok(self):
    self.STATUS_STORE['OK'] += 1

  def __add_fail(self):
    self.STATUS_STORE['FAIL'] += 1

  def __set_last_status(self, result_statuses):
    min_status = min(result_statuses)
    #Change last status if it's not skipped
    self.last_status = min_status if min_status != Status.SKIPPED else self.last_status

  def __get_total(self):
    return self.STATUS_STORE['OK']+self.STATUS_STORE['FAIL']

  def print_results(self, logger=Logger(print)):
    '''Prints STATUSTORE results'''
    logger.log('\n-----------RESULTS-----------', Logger.LogLevel.INFO)
    self.STATUS_STORE['TOTAL'] = self.__get_total() + \
        self.STATUS_STORE['SKIPPED']
    for key in self.STATUS_STORE:
      logger.log(f'{key} : {self.STATUS_STORE[key]}', Logger.LogLevel.INFO)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Main Pipe class

# COMMAND ----------


'''Override list __rshift__ method with the __rshift__ functionality using forbiddenfruit library'''


def rshift_list(self, elem):
  return BaseAgent(self) >> elem


curse(list, "__rshift__", rshift_list)


class BaseAgent():
  '''Main component of pipeline which are executed by Sinbadflow. Pipe object follows a linked list implementation
  with __rshift__ override for specific functionality. Each pipe is wrapped to another Pipe object to implement linked list
  functionality both on single Pipe object and on the list of Pipes objects.
  
  Initialize options:
  data = payload of the Pipe (notebook path)
  trigger = pipe run trigger (see supported triggers in Status IntEnum)
  
  Usage:
  For pipeline creation use '>>' symbols between pipes
  pipeline = BaseAgent() >> BaseAgent()
  
  For parallel run use list of Pipes followed by '>>' symbol
  pipeline = [BaseAgent(),BaseAgent()] >> BaseAgent()
  
  For pipeline concatenation use the same '>>' symbol
  pipeline_x >> pipeline_y
  '''

  def __init__(self, data=None, trigger=Trigger.DEFAULT):
    self.data = data
    self.trigger = trigger
    self.next_elem = None
    self.prev_elem = None

  def __wrap_BaseAgent(self, elem):
    #pipe >> [pipe] case
    if type(elem) == list:
      return BaseAgent(elem)
    #[pipe] >> pipe
    elif type(elem.data) == list:
      return elem
    #pipe >> pipe
    return BaseAgent([elem])

  def __get_head_BaseAgent(self, pipe):
    while pipe.prev_elem is not None:
      pipe = pipe.prev_elem
    return pipe

  def __rshift__(self, elem):
    #This is the first pipe in pipeline, no one wrapped you (root)
    if self.prev_elem == None:
      self = self.__wrap_BaseAgent(self)
    #Wrap the pipe object for single/parallel use, get the head
    wrapped_pipe = self.__get_head_BaseAgent(self.__wrap_BaseAgent(elem))
    self.next_elem = wrapped_pipe
    wrapped_pipe.prev_elem = self
    return wrapped_pipe

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Main Sinbadflow logic

# COMMAND ----------


class Sinbadflow():
  '''Sinbadflow pipeline runner. Named after famous cartoon "Sinbad: Legend of the Seven Seas" it provides ability to run pipelines with databricks notebooks
  and specific triggers in parallel or single mode. The main component of Sinbadflow - BaseAgent() object from which pipelines are build.
  
  Initialize options:
  logging_option = print - selects prefered option of logging (print/logging supported)
  
  Methods:
  run(pipeline: Pipe, timeout=1800) - runs the input pipeline with specific dbutils.notebook.run timeout parameter
  get_head_from_pipeline(pipeline: Pipe) -> BaseAgent(), returns the head pipe form the pipeline
  print_pipeline(pipeline: Pipe) - prints the full pipeline
  
  Usage example:
  pipe_x = BaseAgent('/path/to/notebook', Trigger.OK_PREV)
  pipe_y = BaseAgent('/path/to/notebook', Trigger.FAIL_PREV)
  
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
      pointer = pointer.next_elem if forward else pointer.prev_elem

  def __set_head_BaseAgent(self, pipe):
    if pipe.prev_elem == None:
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
      for result in executor.map(self.__execute, element_list):
        result_statuses.append(result.status)
    self.status_handler.add_status(result_statuses)

  def __execute(self, element):
    if not self.__is_trigger_initiated(element.trigger):
      result_status = Status.SKIPPED
    else:
      try:
        dbutils.notebook.run(element.data, self.timeout)
        result_status = Status.OK
      except Exception as e:
        result_status = Status.FAIL
    return self.__log_and_return_result(result_status, element)

  def __log_and_return_result(self, status, element):
    if status == Status.SKIPPED:
      self.logger.log(f'     SKIPPED: Trigger rule failed for notebook {element.data}: Pipe trigger rule -> {element.trigger.name}' +
                      f' and previous run status -> {self.status_handler.last_status.name}', Logger.LogLevel.WARNING)
      return RunResults(status, element.data)
    else:
      level = Logger.LogLevel.CRITICAL if status == Status.FAIL else Logger.LogLevel.INFO
      self.logger.log(
          f'     Notebook {element.data} run status: {status.name.replace("_PREV","")}', level)
      return RunResults(status, element.data)

  def print_pipeline(self, pipeline):
    '''Prints full visual pipeline'''
    self.head = self.get_head_from_pipeline(pipeline)
    self.logger.log(f'↓     -----START-----')
    self.__traverse_pipeline(self.__print_element)
    self.logger.log(f'■     -----END-----')

  def __print_element(self, elem):
    self.logger.log(
        f'↓     Pipe object {elem} with notebooks to run with triggers {[(pipe.data, pipe.trigger.name) for pipe in elem.data]}')

  def __is_trigger_initiated(self, trigger):
    return self.status_handler.is_status_mapped_to_trigger(trigger)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC How to's. Users interface.

# COMMAND ----------

# sf = Sinbadflow()
# elem1 = BaseAgent('/Users/eimantas.jazukevicius@storebrand.no/parallel_run/fail', Trigger.OK_ALL)
# elem2 = BaseAgent('/Users/eimantas.jazukevicius@storebrand.no/parallel_run/ok', Trigger.OK_PREV)
# elem3 = BaseAgent('/Users/eimantas.jazukevicius@storebrand.no/parallel_run/ok', Trigger.OK_ALL)
# elem4 = BaseAgent('/Users/eimantas.jazukevicius@storebrand.no/parallel_run/ok')
# elem5 = BaseAgent('/Users/eimantas.jazukevicius@storebrand.no/parallel_run/ok')
# elem6 = BaseAgent('/Users/eimantas.jazukevicius@storebrand.no/parallel_run/fail', Trigger.OK_PREV)
# elem7 = BaseAgent('/Users/eimantas.jazukevicius@storebrand.no/parallel_run/ok', Trigger.FAIL_PREV)

# ## We can add pipes together with '>>' notation
# pipeline_one = elem1 >> elem2

# ##We can also use list notation for parallel run
# pipeline_two = [elem3, elem4, elem5] >> elem6 >> elem7

# ##Adding pipelines togther
# summed_pipeline = pipeline_one >> pipeline_two

# COMMAND ----------

#We can print to see the full pipeline

# sf.print_pipeline(summed_pipeline)

# COMMAND ----------

## Or we can run it

# sf.run(summed_pipeline)
