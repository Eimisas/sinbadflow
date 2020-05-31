from enum import Enum
import logging

class LogLevel(Enum):
    INFO = 0
    WARNING = 1
    CRITICAL = 2

class Logger():
  '''Logger used in Sinbadflow pipeline builder. Currently 'print', 'logging' and inner class 'EmptyLogger' functionality is supported.
  
  Initialize options:
    method = selects preferred option of logging (print/logging objects supported)
  
  Methods:
    log(message: string, level=Level.INFO: internal Level enum)
  
  Objects:
    class LogLevel(Enum) - used to select specific log level
    class EmptyLogger - logger object with 'log' method used for testing to keep stdout empty
  
  Usage example:
    lg = Logger(logging)
    lg.log('test', Logger.LogLevel.WARNING) ---> logging.warning('test')
  '''
  
  class EmptyLogger():
    '''Usually - used for testing'''
    def log(message):
      pass
  
  def __init__(self, method):
    self.method = method
    self.level_to_method = {
      print : {
        LogLevel.INFO: print,
        LogLevel.WARNING: print,
        LogLevel.CRITICAL: print
      },
      logging : {
        LogLevel.INFO: logging.info,
        LogLevel.WARNING: logging.warning,
        LogLevel.CRITICAL: logging.error
      },
      Logger.EmptyLogger: {
        LogLevel.INFO: Logger.EmptyLogger.log,
        LogLevel.WARNING: Logger.EmptyLogger.log,
        LogLevel.CRITICAL: Logger.EmptyLogger.log
      }
    }
  
  def log(self, message, level=LogLevel.INFO):
    self.level_to_method[self.method][level](message)
