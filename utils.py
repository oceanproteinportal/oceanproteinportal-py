import uuid
import logging
"""
Utility package for general use functions.
"""

def getLogLevel(level_str):
    """Helper function for setting the logging level"""
    levels = {
        'DEBUG': logging.DEBUG,
        'INFO': logging.INFO,
        'WARNING': logging.WARNING,
        'ERROR': logging.ERROR,
    }
    return levels.get(level_str, logging.WARNING)

def generateGuid(string_value):
    """Generate a GUID for some string value"""
    return str(uuid.uuid3(uuid.NAMESPACE_DNS, string_value))

def yes_or_no(question):
    while "The answer is invalid.":
        reply = str(input(question + ' (y/n): ')).lower().strip()
        if reply[0] == 'y':
            return True
        if reply[0] == 'n':
            return False
