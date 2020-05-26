import logging

def init():
    try:
        dbutils.library.installPyPI('forbiddenfruit')
    except:
        logging.warning('''Library "forbiddenfruit" failed to install using dbutils.
        Install it manually using PIP or check if dbutils.library.installPyPI is present in the scope.''')