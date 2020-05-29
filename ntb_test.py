# Databricks notebook source
# MAGIC %run /helpers/prod/data_helpers/data_helpers

# COMMAND ----------

# MAGIC %run /helpers/dev/pipeline/Sinbadflow

# COMMAND ----------

import json
import random
import datetime
import unittest
from unittest.mock import patch, call
from contextlib import redirect_stdout
import logging
import io

class SinbadflowTester(unittest.TestCase):

  def mock_run(self, notebook_path, timeout):
    if 'fail' in notebook_path:
      raise
    self.run_store[notebook_path] = timeout
    
  def setUp(self):
    dbutils.notebook.run = self.mock_run
    self.sf = Sinbadflow(Logger.EmptyLogger, StatusHandler())
    self.run_store = {}
    self.sh = StatusHandler()
    
  def test_should_run_simple_pipeline(self):
    pipeline = Pipe('ok1') >> Pipe('ok2')
    self.sf.run(pipeline, 1)
    output = {'ok1':1, 'ok2':1}
    self.assertTrue(self.run_store == output , f"Should get {output}, got: {self.run_store}")
    
  def test_should_run_parallel_pipeline(self):
    pipeline = Pipe('ok1') >> [Pipe('ok2'), Pipe('ok3')]
    self.sf.run(pipeline, 2)
    output = {'ok1':2, 'ok2':2, 'ok3': 2}
    self.assertTrue(self.run_store == output , f"Should get {output}, got: {self.run_store}")
    
  def test_should_get_head_from_pipeline(self):
    root = Pipe([Pipe('ok1')])
    pipeline = root >> Pipe('ok2')
    self.sf.run(pipeline, 3)
    self.assertTrue(self.sf.get_head_from_pipeline(pipeline) == root , f"Should get {root}, got: {self.sf.get_head_from_pipeline(pipeline)}")
    
  def test_should_connect_two_pipelines(self):
    pipe1 = Pipe('ok1') >> Pipe('ok2')
    pipe2 = [Pipe('ok3'), Pipe('ok4')] >> Pipe('ok5')
    summed = pipe1 >> pipe2
    self.sf.run(summed, 4)
    output = {'ok1':4, 'ok2':4, 'ok3': 4, 'ok4':4, 'ok5':4}
    self.assertTrue(self.run_store == output , f"Should get {output}, got: {self.run_store}")
    
  def test_should_trigger_fail_all(self):
    pipeline = Pipe('fail') >> Pipe('ok', Trigger.FAIL_ALL)
    self.sf.run(pipeline, 5)
    output = {'ok':5}
    self.assertTrue(self.run_store == output , f"Should get {output}, got: {self.run_store}")
    
  def test_should_trigger_ok_all(self):
    pipeline = Pipe('ok1') >> Pipe('ok2', Trigger.OK_ALL)
    self.sf.run(pipeline, 6)
    output = {'ok1':6, 'ok2':6}
    self.assertTrue(self.run_store == output , f"Should get {output}, got: {self.run_store}")
    
  def test_should_trigger_fail_prev(self):
    pipeline = Pipe('fail') >> Pipe('ok', Trigger.FAIL_PREV)
    self.sf.run(pipeline, 7)
    output = {'ok':7}
    self.assertTrue(self.run_store == output , f"Should get {output}, got: {self.run_store}")
    
  def test_should_trigger_ok_prev(self):
    pipeline = Pipe('ok1') >> Pipe('ok2', Trigger.OK_PREV)
    self.sf.run(pipeline, 8)
    output = {'ok1':8, 'ok2':8}
    self.assertTrue(self.run_store == output , f"Should get {output}, got: {self.run_store}")
    
  def test_should_trigger_done_prev(self):
    pipeline = Pipe('fail') >> Pipe('ok2', Trigger.OK_PREV) >> Pipe('ok3')
    self.sf.run(pipeline, 9)
    output = {'ok3':9}
    self.assertTrue(self.run_store == output , f"Should get {output}, got: {self.run_store}")   
  
  def test_should_trigger_works_parallel_notebooks(self):
    pipeline = Pipe('fail') >> [Pipe('ok1',Trigger.OK_ALL), Pipe('ok2', Trigger.OK_ALL), Pipe('ok3', Trigger.FAIL_PREV)]
    self.sf.run(pipeline, 10)
    output = {'ok3':10}
    self.assertTrue(self.run_store == output , f"Should get {output}, got: {self.run_store}")
    
  def test_should_trigger_works_at_parallel_notebooks_output(self):
    pipeline = [Pipe('fail'), Pipe('ok2')] >> Pipe('ok3', Trigger.FAIL_PREV)
    self.sf.run(pipeline, 11)
    output = {'ok2':11, 'ok3':11}
    self.assertTrue(self.run_store == output , f"Should get {output}, got: {self.run_store}")   
   
  def test_should_skip_all_after_fail(self):
    pipeline = Pipe('fail') >> Pipe('ok2', Trigger.OK_PREV) >> Pipe('ok3', Trigger.OK_ALL) >> [Pipe('fail',Trigger.OK_PREV), Pipe('ok2', Trigger.OK_PREV)]
    self.sf.run(pipeline, 12)
    output = {}
    self.assertTrue(self.run_store == output and self.sf.status_handler.STATUS_STORE['SKIPPED'] == 4, f"Should get {output}, got: {self.run_store}") 
    
  def test_should_connect_pipelines_and_run_all(self):
    pipeline = Pipe('ok1') >> Pipe('ok2') 
    pipeline2 = Pipe('ok3') >> [Pipe('ok4'), Pipe('ok5')]
    pipeline_sum = pipeline >> pipeline2
    self.sf.run(pipeline_sum, 13)
    output = {'ok1':13, 'ok2':13, 'ok3':13, 'ok4':13, 'ok5':13}
    self.assertTrue(self.run_store == output, f"Should get {output}, got: {self.run_store}")    
    
  def test_should_get_correct_statuses(self):
    pipeline = Pipe('fail') >> Pipe('ok2', Trigger.OK_PREV) >> Pipe('ok3', Trigger.FAIL_PREV) >> [Pipe('fail',Trigger.OK_PREV), Pipe('ok2', Trigger.OK_PREV)]
    self.sf.run(pipeline, 14)
    store = self.sf.status_handler.STATUS_STORE
    self.assertTrue(store['SKIPPED'] == 1 and store['FAIL'] == 2 and store['OK'] == 2,
                    f"Should get [skipped:1, fail_prev: 2, ok_prev: 2], got {store['SKIPPED'], store['FAIL'], store['OK']}")
    
  def test_should_do_nothing(self):
    pipeline = Pipe() >> Pipe()
    self.sf.run(pipeline, 15)
    self.assertTrue(self.run_store == {}, f"Should get nothing, got: {self.run_store}")
  
  def test_should_save_and_accumulate_status(self):
    self.sh.add_status([Status.OK, Status.FAIL, Status.OK, Status.OK])
    store = self.sh.STATUS_STORE
    self.assertTrue(self.sh.last_status == Status.FAIL and store['OK'] == 3 and store['FAIL'] == 1,
                    f"Should get last status {Status.FAIL} and 'OK':3, 'FAIL':1 , got: {[self.sh.last_status, store['OK'], store['FAIL']]}")
    
  @patch('builtins.print')  
  def test_should_print_with_level_info(self, mocked_print):
    lg = Logger(print)
    lg.log('info_test', Logger.LogLevel.INFO)
    self.assertTrue([call('info_test')] in mocked_print.mock_calls,
                   f'Should call "info_test", got {mocked_print.mock_calls}')
    
  @patch('builtins.print')  
  def test_should_print_with_level_warning(self, mocked_print):
    lg = Logger(print)
    lg.log('warning_test', Logger.LogLevel.WARNING)
    self.assertTrue([call('warning_test')] in mocked_print.mock_calls,
                   f'Should call "warning_test", got {mocked_print.mock_calls}')
    
  @patch('builtins.print')  
  def test_should_print_with_level_critical(self, mocked_print):
    lg = Logger(print)
    lg.log('critical_test',Logger.LogLevel.CRITICAL)
    self.assertTrue([call('critical_test')] in mocked_print.mock_calls,
                   f'Should call "critical_test", got {mocked_print.mock_calls}')
    
  def test_should_log_info(self):
    with self.assertLogs(level='INFO') as log:
      lg = Logger(logging)
      lg.log('log_info', Logger.LogLevel.INFO)
      self.assertIn('log_info', log.output[0])
  
  def test_should_log_warning(self):
    with self.assertLogs(level='WARNING') as log:
      lg = Logger(logging)
      lg.log('log_warning', Logger.LogLevel.WARNING)
      self.assertIn('log_warning', log.output[0])
      
  def test_should_log_info(self):
    with self.assertLogs(level='ERROR') as log:
      lg = Logger(logging)
      lg.log('log_critical', Logger.LogLevel.CRITICAL)
      self.assertIn('log_critical', log.output[0])
      
  def test_should_not_log_with_empty_logger(self):
    lg = Logger(Logger.EmptyLogger)
    buf = io.StringIO()
    with redirect_stdout(buf):
      lg.log('foobar')
    self.assertNotIn('foobar', buf.getvalue())
  
  def test_should_trigger_be_mapped_to_status(self):
    is_ok_prev_mapped_to_ok_all = self.sh.is_status_mapped_to_trigger(Trigger.OK_PREV)
    is_fail_prev_mapped_to_ok_all = self.sh.is_status_mapped_to_trigger(Trigger.FAIL_PREV)
    self.sh.last_status = Status.FAIL_ALL
    is_ok_prev_mapped_to_fail_all = self.sh.is_status_mapped_to_trigger(Trigger.OK_PREV)
    is_fail_prev_mapped_to_fail_all = self.sh.is_status_mapped_to_trigger(Trigger.FAIL_PREV)
    self.sh.last_status = Status.OK
    is_ok_prev_mapped_to_ok_prev = self.sh.is_status_mapped_to_trigger(Trigger.OK_PREV)
    is_fail_prev_mapped_to_ok_prev = self.sh.is_status_mapped_to_trigger(Trigger.FAIL_PREV)
    self.sh.last_status = Status.FAIL
    is_ok_prev_mapped_to_fail_prev = self.sh.is_status_mapped_to_trigger(Trigger.OK_PREV)
    is_fail_prev_mapped_to_fail_prev = self.sh.is_status_mapped_to_trigger(Trigger.FAIL_PREV)
    self.assertTrue(is_ok_prev_mapped_to_ok_all and
                  not is_fail_prev_mapped_to_ok_all and
                  not is_ok_prev_mapped_to_fail_all and
                  is_fail_prev_mapped_to_fail_all and
                  is_ok_prev_mapped_to_ok_prev and
                  not is_fail_prev_mapped_to_ok_prev and
                  not is_ok_prev_mapped_to_fail_prev and
                  is_fail_prev_mapped_to_fail_prev)

# COMMAND ----------

suite = unittest.TestLoader().loadTestsFromTestCase(SinbadflowTester)
runner = unittest.TextTestRunner(verbosity=2)
runner.run(suite)