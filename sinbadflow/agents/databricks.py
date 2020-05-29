from ..pipe import Pipe
from ..utils import Trigger

class DatabricksNotebook(Pipe):
    def __init__(self, notebook_path, trigger=Trigger.DEFAULT, timeout=1800, args={}):
        self.timeout = timeout
        self.args = args
        super(DatabricksNotebook, self).__init__(notebook_path, trigger)

    # def executor(self):
    #     dbutils.notebook.run(
    #         self.data, self.timeout, self.args)

    def run(self, data):
        dbutils.notebook.run(
            self.data, self.timeout, self.args)
