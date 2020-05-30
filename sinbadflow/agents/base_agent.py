from ..element import Element
from ..utils import Trigger
from abc import ABCMeta, abstractmethod

class BaseAgent(Element, metaclass=ABCMeta):
    def __init__(self, data, trigger=Trigger.DEFAULT):
        super(BaseAgent, self).__init__(data, trigger)

    ## This ensures that derived classes implements run method
    @abstractmethod
    def run(self):
        pass
