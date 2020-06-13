from .utils import Trigger
from forbiddenfruit import curse

'''Override list __rshift__ method with the __rshift__ functionality using forbiddenfruit library'''
def rshift_list(self, elem):
    return Element(self) >> elem
curse(list, "__rshift__", rshift_list)


class Element():
    '''Base component of BaseAgent which are executed by Sinbadflow. Element object follows a linked list implementation
    with __rshift__ override for connection to one another.

    Attributes:
        data: string - payload of the element (notebook path)
        trigger: Trigger - element run trigger (see supported triggers in Status IntEnum)

    Usage example:
    
        For pipeline creation use '>>' symbols between the elements
            pipeline = Element() >> Element()

        For parallel run use list of BaseAgent's followed by '>>' symbol
            pipeline = [Element(),Element()] >> Element()

        For pipeline concatenation use the same '>>' symbol
            pipeline_x >> pipeline_y
    '''

    def __init__(self, data, trigger=Trigger.DEFAULT):
        self.data = data
        self.trigger = trigger
        self.next_elem = None
        self.prev_elem = None

    def __wrap_element(self, elem):
        # elem >> [elem] case
        if type(elem) == list:
            return Element(elem)
        #[elem] >> elem
        elif type(elem.data) == list:
            return elem
        #elem >> elem
        return Element([elem])

    def __get_head_element(self, elem):
        while elem.prev_elem is not None:
            elem = elem.prev_elem
        return elem

    def __rshift__(self, elem):
        # This is the first elem in pipeline, let's wrap you (head)
        if self.prev_elem == None:
            self = self.__wrap_element(self)
        # Wrap the elem object for single/parallel use, get the head
        wrapped_elem = self.__get_head_element(self.__wrap_element(elem))
        self.next_elem = wrapped_elem
        wrapped_elem.prev_elem = self
        return wrapped_elem
