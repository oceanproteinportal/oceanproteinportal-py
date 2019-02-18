import decimal
import json
import datetime
import logging
import elasticsearch
import elasticsearch.helpers
"""
Manage an Elasticsearch data store for the Ocean ProteinPortal
"""

class ElasticStore(DataStore):
    """An Elasticsearch data store.

    Properties:
    - store:        An elasticsearch.Elasticsearch client
    - config:       A dictionary used to configure the store
    - index:        The name of the Elasticsearch index for the store
    - schema_file:  The file path to an Elasticsearch schema
    """

    __index = 'protein-portal'
    __schema_file = '/elasticsearch/mapping.json'

    def __init__(self,
                 host,
                 port,
                 index_name,
                 schema_file_path,
                 http_compress=True,
                 **es_params):

        self.__index = index_name
        self.__schema_file = schema_file_path
        self.__config = {
          'host': host,
          'port': port
        }
        for param in es_params:
            self.__config[param] = es_params[param]

        # Setup an Elasticsearch client
        self.__store = elasticsearch.Elasticsearch(
            hosts=[self.config],
            http_compress=http_compress
        )

def getConfig(self):
    """Return the Elasticsearch configuration"""
    return self.__config

def getStore(self):
    """Return the Elasticsearch client"""
    return self.__store

def getIndex(self):
    """Return the Elasticsearch Index name"""
    return self.__index

def initialize(self):
    """Initialize an Elasticsearch Index for the OceanProteinPortal."""
    es = store.getStore()
    index = store.getIndex()

    # Delete the index, but ignore if not found (404)
    result = es.indices.delete(index=index, ignore=[404])
    if ('status' in result and result['status'] == 404):
        logging.debug('Index did not exist: %s' % (ES_INDEX))
    elif (not 'acknowledged' in result or result['acknowledged'] != True):
        raise Exception("Could not delete the ES index: %s" % (ES_INDEX))
    else:
        logging.info('Deleted Index: %s' % (ES_INDEX))

    # Create the index
    index_properties = json.loads(open(self.__schema_file).read())
    result = es.indices.create(
      index=index,
      body=json.dumps(index_properties, default=elasticDatatypeHandler)
    )
    if (not 'acknowledged' in result or result['acknowledged'] != True):
        raise Exception("Could not create the ES index: %s with properties: %s" % (ES_INDEX, ES_INDEX_SCHEMA))
    else:
        logging.info('Created Index: %s' % (ES_INDEX))
    logging.info("Done!")

def load(data, doc_type, doc_id):
    """Load data into Elasticsearch"""
    es = store.getStore()
    index = store.getIndex()

    doc = json.dumps(data, default=elasticDatatypeHandler)
    logging.debug(doc)
    res = es.index(index=index, doc_type=doc_type, id=doc_id, body=doc)
    return res['result']

def elasticDatatypeHandler(obj):
    """Datatype Handler for Elasticsearch"""
    if isinstance(obj, decimal.Decimal):
        return float(obj)
    elif isinstance(obj, datetime.datetime):
        return obj.isoformat()
    return json.JSONEncoder.default(json.JSONEncoder, obj)
