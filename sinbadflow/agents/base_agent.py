from ..element import Element
from ..utils import Trigger
from abc import ABCMeta, abstractmethod

class BaseAgent(Element, metaclass=ABCMeta):
    '''Base class for agent creation. All agents must inherit from BaseAgent
    
    Args:
        data: Object - payload of the object
        trigger: Trigger - trigger of the agent, Trigger.DEFAULT by default
        conditional_func: function object - conditional function (True/False), default_func by default

    Methods:
        run() - abstractmethod \n
        default_func()
    '''

    def default_func():
        '''Default conditional function'''
        return True

    def __init__(self, data=None, trigger=Trigger.DEFAULT, conditional_func=default_func):
        self.conditional_func = conditional_func
        super(BaseAgent, self).__init__(data, trigger)

    ## This ensures that derived classes implements run method
    @abstractmethod
    def run(self):
        '''Abstract method which every derived class must implement'''
        pass
