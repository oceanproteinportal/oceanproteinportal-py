import datapackage

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

    def loadDatasetMetadata(datapackage, datasetId):
        """Load Dataset Metadata"""
        pass

    def loadProteins(datapackage, datasetId, row_start=0, row_stop=None):
        """Load Protein Data"""
        pass

    def updateDatasetSampleStats(self, datasetId):
        """ Update Dataset with sample statistics"""
        pass

    def loadPeptides(self, datapackage, datasetId, row_start=0, row_stop=None):
        """Load Peptide Data"""
        pass

    def loadProteinsFASTA(self, datapackage, datasetId):
        """Load FASTA Protein Sequences"""
        pass
