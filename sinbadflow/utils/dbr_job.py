import requests
import json
import time
import logging
from ..settings.dbr_vars import *


class RunStatusError(Exception):
    '''Custom exception class used in JobSubmitter class'''
    pass


class RunStatusError(Exception):
    '''Custom exception class used in JobSubmitter class'''
    pass


class WrongModeSelected(Exception):
    '''Custom exception class used in JobSubmitter class'''
    pass


class NoTokenError(Exception):
    '''Custom exception class used in JobSubmitter class'''
    pass


# Native Databricks variable setup - will only work in Databricks environment
try:
    spark = get_spark()
    dbutils = get_dbutils(spark)
except:
    logging.warning(
        '!!! Failed to set dbutils variable which is used to run notebooks on interactive cluster. Make sure you are inside Databricks environment !!!')
# Native Databricks variable setup - will only work in Databricks environment


class JobSubmitter():
    '''JobSubmitter object runs databricks notebook on job or interactive cluster.

    Attributes:
      cluster_mode: string - Databricks cluster mode to run the job (interactive/job supported)
      job_args: dictionary - Databricks notebook arguments

    Methods:
      set_access_token (token: string) (class method) - sets up access token for cluster creation
      submit_notebook(notebook_path: string, timeout: int, args:dict) - submits notebook to job cluster
      get_job_info(run_id: int) - gets the info about specific run_id'''

    __access_token = None
    DATABRICKS_INSTANCE = 'https://westeurope.azuredatabricks.net'
    safety_timeout = None

    def __init__(self, cluster_mode, input_job_args):
        if cluster_mode in ['interactive', 'job']:
            self.cluster_mode = cluster_mode
        else:
            raise WrongModeSelected(
                f'Wrong cluster_mode selected, Dbr object supports "interactive" or "job" modes, {self.cluster_mode} was passed')

        self.__job_args = {
            "new_cluster": {
                "spark_env_vars": {"PYSPARK_PYTHON": "/databricks/python3/bin/python3"},
                'spark_version': '6.4.x-scala2.11',
                'node_type_id': 'Standard_DS3_v2',
                'driver_node_type_id': 'Standard_DS3_v2',
                'num_workers': 1},
            "timeout_seconds": None,
            "notebook_task": {
                "notebook_path": None},
            "notebook_params": {}}
        self.__job_args['new_cluster'].update(input_job_args)

    @classmethod
    def set_access_token(cls, token):
        '''Set up Databricks access token

        Args:
          token: string - Databricks access token
        '''
        cls.__access_token = token

    def submit_notebook(self, notebook_path, timeout, args):
        '''Submits notebook to run with timeout and arguments

        Args:
          notebook_path: string
          timeout: int
          args: dict
        '''
        if self.cluster_mode == 'interactive':
            dbutils.notebook.run(notebook_path, timeout, args)
            return

        if self.__access_token == None:
            raise NoTokenError(
                '\n !!! Access token missing. Use class method JobSubmitter.set_access_token(<TOKEN>) to set class method !!! \n')

        self.__set_notebook_job_args(notebook_path, timeout, args)
        post_resp = self.__submit_job()
        self.safety_timeout = time.time() + timeout * 1.1
        run_status = self.__get_notebook_status(post_resp.json())
        get_resp = self.get_job_info(post_resp.json().get('run_id'))
        if run_status in ['FAILED', 'TIMEDOUT', 'CANCELED', 'SKIPPED', 'INTERNAL_ERROR']:
            raise RunStatusError(
                f'Run {get_resp.json().get("run_id")} FAILED,  status: {run_status}, run notebook: {get_resp.json().get("run_page_url")}')
        return

    def __set_notebook_job_args(self, notebook_path, timeout, args):
        self.__job_args['notebook_task']['notebook_path'] = notebook_path
        self.__job_args['notebook_params'] = args
        self.__job_args['timeout_seconds'] = timeout

    def __submit_job(self):
        return requests.post(f'{self.DATABRICKS_INSTANCE}/api/2.0/jobs/runs/submit', json=self.__job_args, headers={'Authorization': f'Bearer {self.__access_token}'})

    def get_job_info(self, run_id):
        '''Get info from the job cluster with specific run_id

        Args:
          run_id: int

        Returns:
          dict'''
        return requests.get(f'{self.DATABRICKS_INSTANCE}/api/2.0/jobs/runs/get?run_id={run_id}', headers={'Authorization': f'Bearer {self.__access_token}'})

    def __get_notebook_status(self, response):

        while self.get_job_info(response.get('run_id')).json().get('state').get('life_cycle_state') in ['PENDING', 'RUNNING', 'TERMINATING']:
            time.sleep(10)
            if time.time() > self.safety_timeout:
                return 'TIMEDOUT'

        state = self.get_job_info(response.get('run_id')).json().get('state')
        if state.get('life_cycle_state') in ['SKIPPED', 'INTERNAL_ERROR']:
            return state.get('life_cycle_state')
        return state.get('result_state')
