import datapackage
import logging
import re
import sys
import yaml

'''
import pprint
import decimal
import json
import uuid
import re
import datapackage
import datetime
import dateutil.parser

import elasticsearch
import elasticsearch.helpers
import tableschema.exceptions
from tableschema import Table
from Bio import SeqIO'''

if __name__ == "__main__":
    ingest(sys.argv[1:])

def ingest(config_file):
    """Ingest a datapackage"""

    # Read the config file telling you what to do
    cfg = initialize(config_file)

    # Inspect the datapackage
    dp = datapackage.DataPackage(cfg['ingest'].get('datapackage', None))
    if (dp.errors):
        for error in dp.errors:
            logging.error(error)
        raise Exception('Invalid data package')
    # Validate the Datapackage
    try:
        valid = datapackage.validate(dp.descriptor)
    except exceptions.ValidationError as exception:
        for error in datapackage.exception.errors:
            logging.error(error)
        raise Exception('Invalid data package')

    # Generate datasetId
    datasetId = generateDatasetId(datapackage)
    logging.info('Dataset ID: %s' % (datasetId))

    # execute
    store_type = cfg.get('store', None)
    if store_type is None:
        raise Exception('The configuration does not define an ingest store')

    module = __import__('oceanproteinportal.store')
    store_ = getattr(module, store_type)
    store = store_()

    if cfg['ingest'].get('load-dataset-metadata', False):
        logging.info('***** LOADING DATASET METADATA *****')
        store.loadDatasetMetadata(datapackage=dp, datasetId=datasetId)

    if cfg['ingest'].get('load-protein-data', False):
        protein_row_start = cfg['ingest'].get('protein-load-row-start', 0)
        protein_row_stop = cfg['ingest'].get('protein-load-row-stop', None)
        logging.info('***** LOADING PROTEINS (row=%s, %s) *****' % (protein_row_start, protein_row_stop))
        store.loadProteins(datapackage=dp, datasetId=datasetId, row_start=protein_row_start, row_stop=protein_row_stop)

    if cfg['ingest'].get('calculate-dataset-metadata-stats', False):
        logging.info('***** UPDATING DATASET Sample STATS *****')
        store.updateDatasetSampleStats(datasetId=datasetId)

    if cfg['ingest'].get('load-fasta', False):
        logging.info('***** LOAD PROTEIN FASTA *****')
        store.loadProteinsFASTA(datapackage=dp, datasetId=datasetId )

    if cfg['ingest'].get('load-peptide-data', False):
        peptide_row_start = cfg['ingest'].get('peptide-load-row-start', 0)
        peptide_row_stop = cfg['ingest'].get('peptide-load-row-stop', None)
        logging.info('***** LOADING PEPTIDES (row=%s, %s) *****' % (peptide_row_start, peptide_row_stop))
        store.loadPeptide(datapackage=dp, datasetId=datasetId, row_start=peptide_row_start, row_stop=peptide_row_stop)

    if cfg['ingest'].get('add-peptides-to-proteins', False):
        storeupdateProteinsWithPeptide(datapackage=dp, datasetId=datasetId )


def initialize(config_file):
    # Read the configuration
    with open(config_file, 'r') as yamlfile:
        cfg = yaml.load(yamlfile)

    # Setup the logger w. default stream logger
    log_handlers = [logging.StreamHandler()]
    # File Logger
    log_file = cfg['logging'].get('file', None)
    if (None is not log_file):
        log_handlers.append(logging.FileHandler(filename=log_file, mode='a'))

    log_level = oceanproteinportal.utils.getLogLevel(cfg['logging'].get('level', 'WARNING'))
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=log_handlers
    )
    logging.log(log_level, 'Log Level: %s' % (logging.getLevelName(log_level)))
    if (None is not log_file):
        logging.log(log_level, 'Log File: %s' % (log_file))

    # Log the configuration
    logging.log(log_level, '%s' % (cfg))

    # Verify the user wants to ingest
    proceed = oceanproteinportal.utils.yes_or_no('Do you want to continue ingest with this configuration?')
    if proceed is False:
        logging.log(log_level, 'Quitting ingest.')
        sys.exit()

    return cfg


def generateDatasetId(datapackage):
    """Generate a GUID for a Datapackage based on the package name and version"""
    guname = datapackage.descriptor['name'] + '_ver.' + datapackage.descriptor.get('version', 'noversion')
    return oceanproteinportal.utils.generateGuid( guname )

