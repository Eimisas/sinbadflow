from .utils import Trigger
from forbiddenfruit import curse
from .settings import settings

# Library install
settings.init()

'''Override list __rshift__ method with the __rshift__ functionality using forbiddenfruit library'''


def rshift_list(self, elem):
    return Pipe(self) >> elem


curse(list, "__rshift__", rshift_list)


class Pipe():
    '''Main component of pipeline which are executed by Sinbadflow. Pipe object is follows a linked list implementation
    with __rshift__ override for specific functionality.

    Initialize options:
      data = payload of the Pipe (notebook path)
      trigger = pipe run trigger (see supported triggers in Status IntEnum)

    Usage:
    For pipeline creation use '>>' symbols between pipes
      pipeline = Pipe() >> Pipe()

    For parallel run use list of Pipes followed by '>>' symbol
      pipeline = [Pipe(),Pipe()] >> Pipe()

    For pipeline concatenation use the same '>>' symbol
      pipeline_x >> pipeline_y
    '''

    def __init__(self, data=None, trigger=Trigger.DEFAULT):
        self.data = data
        self.trigger = trigger
        self.next_pipe = None
        self.prev_pipe = None

    def __wrap_pipe(self, elem):
        # pipe >> [pipe] case
        if type(elem) == list:
            return Pipe(elem)
        #[pipe] >> pipe
        elif type(elem.data) == list:
            return elem
        #pipe >> pipe
        return Pipe([elem])

    def __get_head_pipe(self, pipe):
        while pipe.prev_pipe is not None:
            pipe = pipe.prev_pipe
        return pipe

    def __rshift__(self, elem):
        # This is the first pipe in pipeline, let's wrap you (head)
        if self.prev_pipe == None:
            self = self.__wrap_pipe(self)
        # Wrap the pipe object for single/parallel use, get the head
        wrapped_pipe = self.__get_head_pipe(self.__wrap_pipe(elem))
        self.next_pipe = wrapped_pipe
        wrapped_pipe.prev_pipe = self
        return wrapped_pipe

    ##Todo executor interface implementation