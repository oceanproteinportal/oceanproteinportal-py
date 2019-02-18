
'''
Utility package for general use functions.
'''

# Helper function for setting the logging level
def getLogLevel(level_str):
    levels = {
        'DEBUG': logging.DEBUG,
        'INFO': logging.INFO,
        'WARNING': logging.WARNING,
        'ERROR': logging.ERROR,
    }
    return levels.get(level_str, logging.WARNING)
