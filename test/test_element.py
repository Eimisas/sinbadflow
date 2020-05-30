import unittest
from sinbadflow.element import Element
from sinbadflow.utils import Status

class ElementTest(unittest.TestCase):

    
    def test_should_connect_two_pipes(self):
        pipe1 = Element('ok1')
        pipe2 = Element('ok2')
        pipeline = pipe1 >> pipe2
        self.assertTrue(pipeline.data[0].data == 'ok2' and pipeline.prev_elem.data[0].data == 'ok1' \
                        and pipeline.prev_elem.data[0].data == 'ok1' and pipeline.prev_elem.next_elem.data[0].data == 'ok2',
                         f"Should get connected pipes")

    def test_should_connect_parallel_to_single_pipes(self):
        pipe = Element('ok1')
        lst_pipe = [Element('ok2', Status.FAIL_ALL), Element('ok3')]
        pipeline_lst_first = lst_pipe >> pipe
        pipeline_single_first = pipe >> lst_pipe
        should_get_ok2 = pipeline_lst_first.prev_elem.data[0].data == 'ok2' and \
             pipeline_lst_first.prev_elem.data[0].trigger == Status.FAIL_ALL
        should_get_ok1 = pipeline_single_first.prev_elem.data[0].data =='ok1'
        should_get_ok3 = pipeline_lst_first.prev_elem.data[1].data == 'ok3'
        self.assertTrue(should_get_ok1 and should_get_ok2 and should_get_ok3,
        f'Expected all True, got {should_get_ok1, should_get_ok2, should_get_ok3}')

    def test_should_pipe_wrap_rshift_empty_list(self):
        pipe = Element('ok')
        pipeline = pipe >> []
        self.assertTrue(pipeline.data == [], f'Should get empty list, got {pipeline.data}')

    def test_should_list_wrap_rshift_Element(self):
        pipeline = [] >> Element('ok')
        self.assertTrue(pipeline.prev_elem.data == [] and pipeline.data[0].data == 'ok',
         f'Should get empty list, got {pipeline.prev_elem.data} and should get "ok", got {pipeline.data[0].data}')


