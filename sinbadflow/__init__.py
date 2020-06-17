'''Sinbadflow pipeline runner. Named after famous cartoon "Sinbad: Legend of the Seven Seas" it provides ability to run pipelines made of agents
    with specific triggers and conditional functions in parallel (using ThreadPoolExecutor) or single mode.'''
from .executor import Sinbadflow
from .utils import StatusHandler, Trigger
