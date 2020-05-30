
import unittest
from sinbadflow.agents.base_agent import BaseAgent


class BaseAgentTest(unittest.TestCase):

    def test_should_throw_implementation_error(self):
        class DummyAgent(BaseAgent):
            def __init__(self, data):
                super(DummyAgent, self).__init__(data)

        def create_obj():
            dm = DummyAgent()

        self.assertRaises(TypeError, create_obj)

    def test_should_succeed_creating_agent(self):
        class DummyAgent(BaseAgent):
            def __init__(self, data):
                super(DummyAgent, self).__init__(data)

            def run(self):
                pass
        dm = DummyAgent('data')

        self.assertTrue(isinstance(dm, DummyAgent),
                        f'Should succeed creating a DummyAgent with run implementation')
