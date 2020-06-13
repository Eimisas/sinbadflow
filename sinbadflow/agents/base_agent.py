from ..element import Element
from ..utils import Trigger
from abc import ABCMeta, abstractmethod

class BaseAgent(Element, metaclass=ABCMeta):
    '''Base class for agent creation. All agents must inherit from BaseAgent
    
    Attributes:
        data=None - payload of the object
        trigger=Trigger.DEFAULT - trigger of the agent
        conditional_func=default_func - conditional function (True/False)

    Methods:
        run(): abstractmethod
    '''

    def default_func():
        return True

    def __init__(self, data=None, trigger=Trigger.DEFAULT, conditional_func=default_func):
        self.conditional_func = conditional_func
        super(BaseAgent, self).__init__(data, trigger)

    ## This ensures that derived classes implements run method
    @abstractmethod
    def run(self):
        pass
