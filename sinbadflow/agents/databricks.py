from ..utils import Trigger
from .base_agent import BaseAgent


class DatabricksNotebook(BaseAgent):
    '''Databricks notebook agent, used to run notebooks using dbutils.notebook.run functionality'''
    def __init__(self, notebook_path, trigger=Trigger.DEFAULT, timeout=1800, args={}):
        self.timeout = timeout
        self.args = args
        super(DatabricksNotebook, self).__init__(notebook_path, trigger)

    def run(self, data):
        dbutils.notebook.run(
            self.data, self.timeout, self.args)
