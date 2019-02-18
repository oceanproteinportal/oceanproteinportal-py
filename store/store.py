"""
Manage a data store for the OceanProteinPortal
"""

class DataStore:
    """A Data Store."""

    __config = None
    __store = None

    def __init__(self):
        __config = None
        __store = None

def getConfig(self):
    """Return the configuration"""
    return self.__config

def getStore(self):
    """Return the store"""
    return self.__store

def initialize(store):
    """Initialize the store."""
    pass

def load(data):
    """Load data into the store."""
    pass
