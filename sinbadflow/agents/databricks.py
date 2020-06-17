from ..utils import Trigger
from .base_agent import BaseAgent
from ..utils.dbr_job import *

class DatabricksAgent(BaseAgent):
    '''Databricks notebook agent, used to run notebooks on interactive or job clusters
    
    Args:
        notebook_path: string
        trigger: Trigger - trigger to run the agent, Trigger.DEFAULT by default
        timeout: int - timeout used for databricks jobs, 7200 by default
        args: dict - arguments passed to databricks jobs, {} by default
        cluster_mode: string - databricks cluster mode selection (interactive/job supported), 'interactive' by default
        job_args: dict - job cluster parameters. Values that can be changed: 'spark_version', 'node_type_id','driver_node_type_id', 'num_workers'. For more information see - https://docs.databricks.com/dev-tools/api/latest/jobs.html

    Methods:
        run()
    '''

    def __init__(self, notebook_path=None, trigger = Trigger.DEFAULT, timeout=7200,
                args={}, cluster_mode='interactive', job_args={}, **kwargs):
        self.notebook_path = notebook_path
        self.trigger = trigger
        self.timeout = timeout
        self.args = args
        self.cluster_mode = cluster_mode
        self.job_args = job_args
        super(DatabricksAgent, self).__init__(notebook_path, trigger, **kwargs)

    def run(self):
        '''Runs the notebook on interactive or job cluster'''
        js = JobSubmitter(self.cluster_mode, self.job_args)
        js.submit_notebook(self.notebook_path, self.timeout, self.args)
