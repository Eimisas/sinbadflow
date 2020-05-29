import unittest
from sinbadflow.pipe import Pipe
from sinbadflow.utils import Status

class PipeTest(unittest.TestCase):

    
    def test_should_connect_two_pipes(self):
        pipe1 = Pipe('ok1')
        pipe2 = Pipe('ok2')
        pipeline = pipe1 >> pipe2
        self.assertTrue(pipeline.data[0].data == 'ok2' and pipeline.prev_pipe.data[0].data == 'ok1' \
                        and pipeline.prev_pipe.data[0].data == 'ok1' and pipeline.prev_pipe.next_pipe.data[0].data == 'ok2',
                         f"Should get connected pipes")

    def test_should_connect_parallel_to_single_pipes(self):
        pipe = Pipe('ok1')
        lst_pipe = [Pipe('ok2', Status.FAIL_ALL), Pipe('ok3')]
        pipeline_lst_first = lst_pipe >> pipe
        pipeline_single_first = pipe >> lst_pipe
        should_get_ok2 = pipeline_lst_first.prev_pipe.data[0].data == 'ok2' and \
             pipeline_lst_first.prev_pipe.data[0].trigger == Status.FAIL_ALL
        should_get_ok1 = pipeline_single_first.prev_pipe.data[0].data =='ok1'
        should_get_ok3 = pipeline_lst_first.prev_pipe.data[1].data == 'ok3'
        self.assertTrue(should_get_ok1 and should_get_ok2 and should_get_ok3,
        f'Expected all True, got {should_get_ok1, should_get_ok2, should_get_ok3}')

    def test_should_pipe_wrap_rshift_empty_list(self):
        pipe = Pipe('ok')
        pipeline = pipe >> []
        self.assertTrue(pipeline.data == [], f'Should get empty list, got {pipeline.data}')

    def test_should_list_wrap_rshift_pipe(self):
        pipeline = [] >> Pipe('ok')
        self.assertTrue(pipeline.prev_pipe.data == [] and pipeline.data[0].data == 'ok',
         f'Should get empty list, got {pipeline.prev_pipe.data} and should get "ok", got {pipeline.data[0].data}')             