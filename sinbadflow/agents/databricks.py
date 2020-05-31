from ..utils import Trigger
from .base_agent import BaseAgent
from ..settings import settings

# Library install
settings.init()

class DatabricksAgent(BaseAgent):
    '''Databricks notebook agent, used to run notebooks using dbutils.notebook.run functionality'''
    def __init__(self, notebook_path, trigger=Trigger.DEFAULT, timeout=1800, args={}):
        self.timeout = timeout
        self.args = args
        super(DatabricksAgent, self).__init__(notebook_path, trigger)

    def run(self):
        dbutils.notebook.run(
            self.data, self.timeout, self.args)
