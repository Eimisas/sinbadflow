
import unittest
from sinbadflow.agents.base_agent import BaseAgent


class TestAgent(BaseAgent):
        def run(self):
            pass

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

    def test_should_return_conditional_func(self):
        def f():
            return 100
        d = TestAgent('data', conditional_func=f)
        self.assertTrue(d.conditional_func() == 100,
                        f'Should return 100, got {d.conditional_func()}')
