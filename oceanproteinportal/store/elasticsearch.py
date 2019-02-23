from Bio import SeqIO
import decimal
import datapackage
import datetime
import elasticsearch
import elasticsearch.helpers
import json
import logging
import tableschema.exceptions
from tableschema import Table
import yaml
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

    # Default values that should be overriden
    __index = 'protein-portal'
    __schema_file = '/elasticsearch/mapping.json'
    __config = None
    __store = None

    def __init__(self, host, port, index_name, schema_file_path, http_compress=True, **es_params):
        self.__index = index_name
        self.__schema_file = schema_file_path

        # Config
        self.__config = {
          'host': host,
          'port': port
        }
        for param in es_params:
            self.__config[param] = es_params[param]

        # Store - Setup an Elasticsearch client
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
        es = self.getStore()
        index = self.getIndex()

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

    def load(self, data, type, id):
        """Load data into Elasticsearch"""
        es = self.getStore()
        index = self.getIndex()

        doc = json.dumps(data, default=elasticDatatypeHandler)
        logging.debug(doc)
        res = es.index(index=index, doc_type=type, id=id, body=doc)
        return res['result']

    def loadDatasetMetadata(self, datapackage, datasetId):
        """Load Dataset Metadata"""
        es = self.getStore()
        index = self.getIndex()
        data = {}
        try:
            dataset_doc = es.get(index=index, doc_type='dataset', id=datasetId)
            data = dataset_doc['_source']
        except elasticsearch.exceptions.NotFoundError as exc:
            # New dataset
            data['guid'] = datasetId
            # Cruises for new dataset
            cruises = oceanproteinportal.datapackage.datapackageCruises(datapackage)
            if cruises is not None:
                data['cruises'] = []
                for cruiseId, cruise in cruises.items():
                    label = cruise.get('name')
                    uri = cruise.get('uri', None)
                    data['cruises'].append(cruise)

        # Fields that should be added on new or existing dataset
        data['name'] = datapackage.descriptor.get('title', datapackage.descriptor['name'])
        data['opp:shortName'] = datapackage.descriptor.get('opp:shortName', None)
        data['description'] = datapackage.descriptor.get('description', None)
        data['homepage'] = datapackage.descriptor.get('homepage', None)
        data['version'] = datapackage.descriptor.get('version', None)
        if 'contributors' in datapackage.descriptor:
            data['contributors'] = []
            for contributor in datapackage.descriptor['contributors']:
                if 'title' not in contributor:
                    continue
                name = contributor['title']
                role = contributor.get('role', None)
                uri = contributor.get('uri', None)
                orcid = contributor.get('orcid', None)
                data['contributors'].append({'name': name, 'role': role, 'orcid': orcid, 'uri': uri})
        if 'keywords' in datapackage.descriptor:
            data['keywords'] = []
            for keyword in datapackage.descriptor['keywords']:
                data['keywords'].append(keyword)

        # Load into Elasticsearch
        result = self.load(data=data, type='dataset', id=datasetId)
        logging.info('%s - %s', % (datasetId, result))

    def loadProteins(self, datapackage, datasetId, row_start=0, row_stop=None):
        """Load Protein Data

        Tabular data, so proteins may be repeated for different samples, stations, depths, etc.
        1) Build proteinId first, then lookup if it exists in the store
        2) If not exists, build a new document. Else, update the spectral counts of existing doc
        """
        es = self.getStore()
        index = self.getIndex()

        # Get the Ontology Version
        ontology_version = oceanproteinportal.datapackage.getDatapackageOntologyVersion(datapackage)

        proteinResource = oceanproteinportal.datapackage.findResource(datapackage=datapackage, resource_type='protein')
        if proteinResource is None:
            return

        datasetCruises = oceanproteinportal.datapackage.datapackageCruises(datapackage)
        table = Table(proteinResource.descriptor['path'], schema=proteinResource.descriptor['schema'])

        if (0 < row_start):
            logging.info("Skipping rows until # %s" % (row_start))

        row_count = 0
        proteinId = None
        data = None
        PROTEIN_FIELDS = getOntologyMappingFields(type='protein', ontology_version=ontology_version)
        try:
            for keyed_row in table.iter(keyed=True):
                row_count += 1
                if row_count < row_start:
                    logging.debug("Skipping Row # %s" % (row_count))
                    continue
                if row_stop is not None and row_count > row_stop:
                    logging.info("Stopping at Row# %s" % (row_count))
                    break
                logging.debug("Reading Row# %s" % (row_count))
                row = readKeyedTableRow(keyed_row=keyed_row, elastic_mappings=PROTEIN_FIELDS)

                # Get the unqiue identifier for this protein
                proteinId = row['proteinId']
                protein_guid = generateGuid( datapackage.descriptor['name'] + '_protein_' + datasetId + ':' + proteinId )

                try:
                    res = es.get(index=index, doc_type='protein', id=protein_guid)
                    # Reuse existing protein document
                    data = res['_source']
                except elasticsearch.exceptions.NotFoundError as exc:
                    # Build a new ES Protein document
                    data = {
                      '_dataset': datasetId,
                      'guid': protein_guid,
                      'proteinId': proteinId,
                      'spectralCount': []
                    }

                    if row['productName'] is not None:
                        data['productName'] = row['productName']
                    if row.get('molecularWeight', None) is not None:
                        data['molecularWeight'] = row['molecularWeight']
                    if row.get('enzymeCommId', None) is not None:
                        data['enzymeCommId'] = row['enzymeCommId']
                    if row.get('uniprotId', None) is not None:
                        data['uniprotId'] = row['uniprotId']
                    if row.get('otherIdentifiedProteins', None) is not None:
                        data['otherIdentifiedProteins'] = row['otherIdentifiedProteins']

                    # NCBI
                    ncbiTaxon = None
                    if 'ncbi:id' in row:
                        ncbiTaxon = {
                          'id': row['ncbi:id'],
                          'name': None
                        }
                    if 'ncbi:name' in row:
                        ncbiTaxon['name'] = row['ncbi:name']
                    if ncbiTaxon is not None:
                        data['ncbiTaxon'] = ncbiTaxon

                    # Kegg
                    kegg_pathway = None
                    pathway = row.get('kegg:path', None)
                    if pathway is not None:
                        kegg_pathway = []
                        for idx,path in enumerate(pathway):
                            kegg_pathway.append({'value': path, 'index': idx})
                        data['kegg'] = {
                          'id': row.get('kegg:id', None),
                          'description': row.get('kegg:desc', None),
                          'pathway': kegg_pathway
                        }

                    # PFams
                    if 'pfams:id' in row:
                        data['pfams'] = {
                          'id': data.get('pfams:id', None),
                          'name': data.get('pfams:name', None)
                        }
                # END of initial protein data setup

                # Handle all the unqiue row data for a certain protein
                # FilterSize
                filterSize = {}
                minimumFilterSize = row.get('filterSize:minimum', None)
                maximumFilterSize = row.get('filterSize:maximum', None)
                filterSizeLabel = ''
                if minimumFilterSize is not None:
                    filterSize['minimum'] = minimumFilterSize
                    filterSizeLabel += str(minimumFilterSize)
                if maximumFilterSize is not None:
                    filterSize['maximum'] = maximumFilterSize
                    if filterSizeLabel != '':
                        filterSizeLabel += ' - ' + str(maximumFilterSize)
                    else:
                        filterSize += str(maximumFilterSize)
                if filterSizeLabel != '':
                    filterSize['label'] = filterSizeLabel
                    data['filterSize'] = filterSize

                # Cruise
                cruise = {
                  'value': row.get('spectralCount:cruise', None),
                }
                if datasetCruises is not None and cruise['value'] in datasetCruises:
                    cruise['uri'] = datasetCruises[cruise['value']]['uri']
                # To-do
                # 1. Lookup the cruise URI in the datapackage

                # Spectral Counts
                # fix ISO DateTime
                observationDateTime = None
                if 'spectralCount:dateTime' in row and row['spectralCount:dateTime'] is not None:
                    observationDateTime = dateutil.parser.parse(row['spectralCount:dateTime'])
                    observationDateTime = observationDateTime.strftime(SPECTRAL_COUNT_DATE_TIME_FORMAT)
                elif 'spectralCount:date' in row and row['spectralCount:date'] is not None:
                    time = row.get('spectralCount:time', None)
                    if (time is None):
                        time = '00:00:00'
                    observationDateTime = dateutil.parser.parse(row['spectralCount:date'] + 'T' + time)
                    observationDateTime = observationDateTime.strftime(SPECTRAL_COUNT_DATE_TIME_FORMAT)

                spectralCount = {
                    'sampleId': row.get('spectralCount:sampleId', None),
                    'count': row.get('spectralCount:count', None),
                    'cruise': cruise,
                    'station': row.get('spectralCount:station', None),
                    'depth': row.get('spectralCount:depth', None),
                    'dateTime': observationDateTime,
                }
                if (spectralCount['depth'] is not None):
                    if 'min' not in dataset_depth_stats:
                        dataset_depth_stats['min'] = spectralCount['depth']
                        dataset_depth_stats['max'] = spectralCount['depth']
                    else:
                        if spectralCount['depth'] < dataset_depth_stats['min']:
                            dataset_depth_stats['min'] = spectralCount['depth']
                        if spectralCount['depth'] > dataset_depth_stats['max']:
                            dataset_depth_stats['max'] = spectralCount['depth']

                if (row['spectralCount:coordinate:lat'] is not None and row['spectralCount:coordinate:lon'] is not None):
                    spectralCount['coordinate'] = {
                      'lat': row['spectralCount:coordinate:lat'],
                      'lon': row['spectralCount:coordinate:lon']
                    }
                data['spectralCount'].append(spectralCount)
                res = self.load(data=data, type='protein', id=data['guid'])
                logging.info(res['result'])
            # end of for loop of protein rows
        except Exception as e:
            logging.exception("Error with row[%s]: %s" % (row_count, keyed_row))
            raise e

    def updateDatasetSampleStats(self, datasetId):
        """ Update Dataset with sample statistics"""
        # Get existing dataset document
        es = self.getStore()
        index = self.getIndex()
        dataset_doc = es.get(index=index, doc_type='dataset', id=datasetId)

        # dataset update object
        dataset = {}

        # Min/Max Depths
        depth_aggs = {
          "size": 0,
          "query": {
            "bool": {
              "must": [
                { "match": { "_dataset": datasetId } }
              ]
            }
          },
          "aggs": {
            "depth": {
              "nested": {"path": "spectralCount"},
              "aggs": {
                "maximum": {
                  "max": {"field": "spectralCount.depth"}
                },
                "minimum": {
                  "min": {"field": "spectralCount.depth"}
                }
              }
            }
          }
        }
        res = es.search(index=index, doc_type='protein', body=depth_aggs)
        if len(res['aggregations']['depth']) > 0:
            dataset['depth_stats'] = {
                'max': res['aggregations']['depth']['maximum']['value'],
                'min': res['aggregations']['depth']['minimum']['value']
            }
            logging.info('Depth stats: %s' % (dataset['depth_stats']))

        # Filter Sizes
        filter_size_aggs = {
          "size": 0,
          "query": {
            "bool": {
              "must": [
                { "match": { "_dataset": datasetId } }
              ]
            }
          },
          "aggs": {
            "filter_size": {
              "terms": {"field": "filterSize.label"},
              "aggs":{
                "minimum": {
                  "terms": {"field": "filterSize.minimum"}
                },
                "maximum": {
                  "terms": {"field": "filterSize.maximum"}
                }
              }
            }
          }
        }
        res = es.search(index=index, doc_type='protein', body=filter_size_aggs)
        if len(res['aggregations']['filter_size']['buckets']) > 0:
            filters = []
            for agg_filter in res['aggregations']['filter_size']['buckets']:
                filters.append({
                  'label': agg_filter['key'],
                  'maximum': agg_filter['maximum']['buckets'][0]['key'],
                  'minimum': agg_filter['minimum']['buckets'][0]['key']
                })
            dataset['filterSize'] = filters
            logging.info('Filter stats: %s' % (dataset['filterSize']))

        # Cruise Stations
        cruise_aggs = {
          "size": 0,
          "query": {
            "bool": {
              "must": [
                { "match": { "_dataset": datasetId } }
              ]
            }
          },
          "aggs": {
            "data": {
              "nested": {"path": "spectralCount"},
              "aggs": {
                "cruises": {
                  "terms": {"field": "spectralCount.cruise.value.exact"},
                  "aggs":{
                    "stations": {
                      "terms": {"field": "spectralCount.station"}
                    }
                  }
                }
              }
            }
          }
        }
        res = es.search(index=index, doc_type='protein', body=cruise_aggs)
        if len(res['aggregations']['data']['cruises']['buckets']) > 0:
            cruises = []
            logging.info('Cruises to grab stations for: %s' % (res['aggregations']['data']['cruises']['buckets']))
            for agg_cruise in res['aggregations']['data']['cruises']['buckets']:
                cruise = {'label': agg_cruise['key']}
                if dataset_doc['_source'].get('cruises', None) is not None:
                    for existing_cruise in dataset_doc['_source']['cruises']:
                        if existing_cruise['label'] == cruise['label']:
                            cruise = existing_cruise
                            break

                # Get station names
                lookup_station_coordinates = []
                for agg_cruise_station in agg_cruise['stations']['buckets']:
                    lookup_station_coordinates.append(agg_cruise_station['key'])
                logging.info(lookup_station_coordinates)
                cruise_stations = []
                # Lookup station coordinates
                while (len(lookup_station_coordinates) > 0):
                    # lookup coordinates per stations
                    station_search = {
                      "size": 1,
                      "_source": ["spectralCount.coordinate", "spectralCount.station"],
                      "query": {
                        "bool": {
                          "must": [
                            { "match": { "_dataset": datasetId } },
                            {
                              "nested": {
                                "path": "spectralCount",
                                "query": {
                                  "bool": {
                                    "must":[
                                      { "match": { "spectralCount.cruise.value.exact": agg_cruise['key'] } },
                                      { "match": { "spectralCount.station": lookup_station_coordinates[0] } }
                                    ]
                                  }
                                }
                              }
                            }
                          ]
                        }
                      }
                    }
                    logging.info(station_search)
                    station_locations = elastic.search(index=ES_INDEX, doc_type='protein', body=station_search)
                    if (station_locations['hits']['total'] > 0):
                        for station in station_locations['hits']['hits'][0]['_source']['spectralCount']:
                            logging.info('%s => %s' % (station['station'], lookup_station_coordinates))
                            if station['station'] not in lookup_station_coordinates:
                                continue
                            # Get the coordinates
                            cruise_stations.append({
                              'label': station['station'],
                              'latitude': station['coordinate']['lat'],
                              'longitude': station['coordinate']['lon']
                            })
                            lookup_station_coordinates.remove(station['station'])

                logging.info(cruise_stations)
                cruise['station'] = cruise_stations
                cruises.append(cruise)
            dataset['cruises'] = cruises

        # Update the dataset
        res = self.load(data={'doc': dataset}, type='dataset', id=datasetId)
        logging.info(res['result'])

    def loadProteinsFASTA(self, datapackage, datasetId):
        """Load Proteins FASTA Data"""
        es = self.getStore()
        index = self.getIndex()

        # Get the Ontology Version
        ontology_version = oceanproteinportal.datapackage.getDatapackageOntologyVersion(datapackage)

        fastaResource = oceanproteinportal.datapackage.findResource(datapackage=datapackage, resource_type='fasta')
        if fastaResource is None:
            return

        for record in SeqIO.parse(fastaResource.descriptor['path'], "fasta"):
            results = es.search(
              scroll="5s",
              size=1,
              body={"query":{"bool":{"must":[{"match":{"proteinId.exact": record.id}},{"match":{"_dataset": datasetId}}]}}},
              index=index,
              doc_type="protein",
              filter_path=['hits.hits._id']
            )
            if results:
                if results['hits']['hits']:
                    for result in results['hits']['hits']:
                        update = es.update(
                          index=index,
                          doc_type="protein",
                          id=result['_id'],
                          body={"doc":{"fullSequence":str(record.seq)}},
                          _source=["fullSequence"]
                        )
                        logging.info(update['result'])
            else:
                logging.error('*** NOT FOUND: %s - %s' % (record.id, str(record.seq)))

    def loadPeptides(self, datapackage, datasetId, row_start=0, row_stop=None):
        """Load Peptide Data"""

        # Get the Ontology Version
        ontology_version = oceanproteinportal.datapackage.getDatapackageOntologyVersion(datapackage)

        peptideResource = oceanproteinportal.datapackage.findResource(datapackage=datapackage, resource_type='peptide')
        if peptideResource is None:
            return

        datasetCruises = datapackageCruises(datapackage)
        table = Table( peptideResource.descriptor['path'], schema=peptideResource.descriptor['schema'] )

        if (0 < row_start):
            logging.info("Skipping rows until # %s" % (row_start))

        row_count = 0
        data = None
        PEPTIDE_FIELDS = getOntologyMappingFields(type='peptide', ontology_version=ontology_version)
        for keyed_row in table.iter(keyed=True):
            row_count += 1
            if row_count < row_start:
                logging.debug("Skipping Row # %s" % (row_count))
                continue
            if row_stop is not None and row_count > row_stop:
                logging.info("Stopping at Row# %s" % (row_count))
                break
            logging.debug("Reading Row# %s" % (row_count))
            data = readKeyedTableRow(keyed_row=keyed_row, elastic_mappings=PEPTIDE_FIELDS)
            primaryKey = datasetId + data.get('sampleName') + data.get('proteinId') + data.get('peptideSequence')
            data['guid'] = generateGuid( datapackage.descriptor['name'] + '_peptide_' + primaryKey )

            filterSize = {}
            minimumFilterSize = data.get('filterSize:minimum', None)
            maximumFilterSize = data.get('filterSize:maximum', None)
            filterSizeLabel = ''
            if minimumFilterSize is not None:
                del data['filterSize:minimum']
                filterSize['minimum'] = minimumFilterSize
                filterSizeLabel += str(minimumFilterSize)
            if maximumFilterSize is not None:
                del data['filterSize:maximum']
                filterSize['maximum'] = maximumFilterSize
                if filterSizeLabel != '':
                    filterSizeLabel += ' - ' + str(maximumFilterSize)
                else:
                    filterSize += str(maximumFilterSize)
            filterSize['label'] = filterSizeLabel
            data['filterSize'] = filterSize

            if ('coordinate:lat' in data and 'coordinate:lon' in data):
                data['coordinate'] = {
                  'lat': data['coordinate:lat'],
                  'lon': data['coordinate:lon']
                }
                del data['coordinate:lat']
                del data['coordinate:lon']

            # load in ES
            self.load(data=data, type='peptide', id=data['guid'])
            logging.info(res['result'])

    def updateProteinsWithPeptide(self, datapackage, datasetId):
        """Update Proteins with their peptides"""
        es = self.getStore()
        index = self.getIndex()

        for result in elasticsearch.helpers.scan(
            es,
            scroll="10m",
            size=20,
            query={"query":{"bool":{"must":[{"match":{"_dataset": datasetId}}]}}, "_source": ["proteinId"]},
            index=index,
            doc_type="protein"
        ):
            protein_doc_id = result['_id']
            protein_id = result['_source']['proteinId']
            # find its peptides
            sequences = []
            for result in elasticsearch.helpers.scan(
                es,
                scroll="2m",
                size=20,
                query={"query":{"bool":{"must":[{"match":{"identifiedProteins.exact": protein_id}},{"match":{"_dataset": datasetId}}]}}, "_source": ["peptideSequence"]},
                index=index,
                doc_type="peptide"
            ):
                peptideSequence = result['_source']['peptideSequence']
                if peptideSequence not in sequences:
                    sequences.append(peptideSequence)

            if sequences:
                # update the protein
                logging.debug('Protein Doc: %s, Protein ID: %s, Sequences %s' % (protein_doc_id, protein_id, sequences))
                res = self.load(data={"doc":{"peptideSequence":sequences}}, type='protein', id=protein_doc_id)
                logging.info(res['result'])
                """update = es.update(
                      index=index,
                      doc_type="protein",
                      id=protein_doc_id,
                      body={"doc":{"peptideSequence":sequences}},
                      _source=["peptideSequence"]
                )
                logging.info(update['result'])"""


def readKeyedTableRow(keyed_row, elastic_mappings):
    """Process a keyed table row"""
    row = {}
    for field_name, field_value in keyed_row.items():
        field = table.schema.get_field(field_name)
        if (None is field or
          'rdfType' not in field.descriptor or
          field.descriptor['rdfType'] not in elastic_mappings):
              continue

        field_type = elastic_mappings[field.descriptor['rdfType']]
        processed_value = oceanproteinportal.datapackage.processField(value=field_value, descriptor=field.descriptor, field_type=field.descriptor['rdfType'])

        # handle ES arrays
        if field_type not in row:
            row[field_type] = processed_value
        elif isinstance(row[field_type], list):
            row[field_type].append(processed_value)
        else:
            existing_data_value = row[field_type]
            row[field_type] = [existing_data_value, processed_value]
    return row

def getOntologyMappingFields(config_file='config/ontology_elasticsearch_mappings.yaml', type, ontology_version)
    """Read how the ontology maps to Elasticsearch."""
    # Read the configuration
    with open(config_file, 'r') as yamlfile:
        mappings = yaml.load(yamlfile)
    return mappings[ontology_version][type]

def elasticDatatypeHandler(obj):
    """Datatype Handler for Elasticsearch"""
    if isinstance(obj, decimal.Decimal):
        return float(obj)
    elif isinstance(obj, datetime.datetime):
        return obj.isoformat()
    return json.JSONEncoder.default(json.JSONEncoder, obj)
