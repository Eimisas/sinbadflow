from ..element import Element
from ..utils import Trigger
from abc import ABCMeta, abstractmethod

class BaseAgent(Element, metaclass=ABCMeta):
    '''Base class for agent creation. All agents must inherit from BaseAgent'''

    def func():
        return True

    def __init__(self, data=None, trigger=Trigger.DEFAULT, conditional_func = func):
        self.is_conditional_func_passed = conditional_func
        super(BaseAgent, self).__init__(data, trigger)

    ## This ensures that derived classes implements run method
    @abstractmethod
    def run(self):
        pass
