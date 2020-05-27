from ..pipe import Pipe
from ..status_handler import Status

class DbrNotebook(Pipe):
    def __init__(self, notebook_path, trigger=Status.DONE_PREV, timeout=1800, args=None):
        self.timeout = timeout
        self.args = args
        super(DbrNotebook, self).__init__(notebook_path, trigger)

def run_dbr_notebook(ntb_obj):
    dbutils.notebook.run(ntb_obj.data, ntb_obj.timeout, ntb_obj.args)