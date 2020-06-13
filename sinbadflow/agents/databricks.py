from ..utils import Trigger
from .base_agent import BaseAgent
from ..settings import dbr_settings
from ..utils.dbr_job import *

# Library install with dbutils
dbr_settings.init()

class DatabricksAgent(BaseAgent):
    '''Databricks notebook agent, used to run notebooks on interactive or job clusters
    
    Attributes:
        notebook_path: string
        trigger=Trigger.DEFAULT - trigger to run the agent
        timeout=1800 - timeout used for databricks jobs
        args={} - arguments passed to databricks jobs
        cluster_mode='interactive' - databricks cluster mode selection (interactive/job supported)
        job_args={"spark_version": "6.4.x-scala2.11", "node_type_id": "Standard_DS3_v2", "num_workers": 2} - default job cluster parameters

    Methods:
        run()
    '''

    def __init__(self, notebook_path, trigger = Trigger.DEFAULT, timeout=1800,
                args={}, cluster_mode='interactive',
                job_args={"spark_version": "6.4.x-scala2.11", "node_type_id": "Standard_DS3_v2", "num_workers": 2}, **kwargs):
        self.timeout = timeout
        self.args = args
        self.cluster_mode = cluster_mode
        self.job_args = job_args
        super(DatabricksAgent, self).__init__(notebook_path, trigger, **kwargs)

    def run(self):
        '''Runs the notebook on interactive or job cluster'''
        js = JobSubmitter(self.cluster_mode, self.job_args)
        js.submit_notebook(self.notebook_path, self.timeout, self.args)
