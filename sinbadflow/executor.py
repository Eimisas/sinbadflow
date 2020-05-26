from .logger import Logger, LogLevel
from .status_handler import StatusHandler, Status
from concurrent.futures import ThreadPoolExecutor, wait
from collections import namedtuple

RunResults = namedtuple('RunResults', ['status', 'path'])

class Sinbadflow():
  '''Main component of pipeline which are executed by Sinbadflow. Pipe object follows a linked list implementation
  with __rshift__ override for specific functionality. Each pipe is wrapped to another Pipe object to implement linked list
  functionality both on single Pipe object and on the list of Pipes objects.
  
  Initialize options:
    data = payload of the Pipe (notebook path)
    trigger = pipe run trigger (see supported triggers in Status IntEnum)
  
  Usage:
  For pipeline creation use '>>' symbols between pipes
    pipeline = Pipe() >> Pipe()
  
  For parallel run use list of Pipes followed by '>>' symbol
    pipeline = [Pipe(),Pipe()] >> Pipe()
  
  For pipeline concatenation use the same '>>' symbol
    pipeline_x >> pipeline_y
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
    self.status_handler.log_results(self.logger)

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
    self.__execute(triggered_notebooks) if triggered_notebooks != [] else None

  def __get_notebooks_to_execute(self, elem):
    triggered_notebooks=[]
    for pipe in elem.data:
      if pipe.data == None:
        continue
      if self.__is_trigger_check_passed(pipe.trigger):
        triggered_notebooks.append(pipe.data)
      else:
        self.logger.log(f'   SKIPPED: Trigger rule failed for notebook {pipe.data}: Pipe trigger rule -> {pipe.trigger.name}' +
        f' and previous run status -> {self.status_handler.last_status.name}', LogLevel.WARNING)
        self.status_handler.add_status([Status.DONE_PREV]) #Status.DONE_PREV is acting like "Skipped" in this case
    return triggered_notebooks
  
  def __execute(self, notebook_list):
    self.logger.log(f'   Running notebook(s): {notebook_list}')
    result_statuses = []
    with ThreadPoolExecutor(max_workers=None) as executor: 
      for result in executor.map(self.__run_dbr_notebook, notebook_list):
        result_statuses.append(result.status)
    self.status_handler.add_status(result_statuses)    

  def __run_dbr_notebook(self, path):
    try:
      dbutils.notebook.run(path, self.timeout)
      self.logger.log(f'     Notebook {path} run status: {Status.OK_PREV.name.replace("_PREV","")}')
      return RunResults(Status.OK_PREV, path)
    except Exception as e:
      self.logger.log(f'     Notebook {path} run status: {Status.FAIL_PREV.name.replace("_PREV","")}', LogLevel.CRITICAL)
      return RunResults(Status.FAIL_PREV, path)
  
  def print_pipeline(self, pipeline):
    '''Prints full visual pipeline'''
    self.head = self.get_head_from_pipeline(pipeline)
    self.logger.log(f'↓     -----START-----')
    self.__traverse_pipeline(self.__print_element)
    self.logger.log(f'■     -----END-----')
    
  def __print_element(self, elem):
    self.logger.log(f'↓     Pipe object {elem} with notebooks to run with triggers {[(pipe.data, pipe.trigger.name) for pipe in elem.data]}')
  
  def __is_trigger_check_passed(self, trigger):
    return self.status_handler.is_status_mapped_to_trigger(trigger)