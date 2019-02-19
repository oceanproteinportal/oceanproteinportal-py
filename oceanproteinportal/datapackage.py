import logging
import re
import string
import sys
import yaml
from datapackage import *
"""
Create a Frictionlessdata Data Package for the OceanProteinPortal.
"""

DATAPACKAGE_ONTOLOGY_KEY = '_ontology'

if __name__ == "__main__":
    buildTabularPackage(sys.argv[1])

def buildTabularPackage(config_file):
    """Build a tabular OPP DataPackage.

    See examples/sample-datapackage-confgi.yaml"""

    # Read the configuration
    with open(config_file, 'r') as yamlfile:
        pkg_descriptor = yaml.load(yamlfile)

    # Required metadata
    submission_name = pkg_descriptor.get('name', None)
    if submission_name is None:
        raiseException('Submission Name is required.')
    version_number = pkg_descriptor.get('version', None)
    if version_number is None:
        raiseException('Version Number is required.')
    # Files
    if pkg_descriptor.get('files', None) is None:
        raiseException('Missing files description')
    protein_data = pkg_descriptor['files'].get('protein')
    if protein_data is None:
        raiseException('Path to protein spectral counts is required.')
    peptide_data = pkg_descriptor['files'].get('peptide')
    if peptide_data is None:
        raiseException('Path to peptide spectral counts is required.')
    fasta_data = pkg_descriptor['files'].get('fasta')
    if fasta_data is None:
        raiseException('Path to protein FASTA is required.')

    #remove the files from the config for the datapackage descriptor
    del pkg_descriptor['files']

    # Data provider tells us which ontology they used
    ontology_version = pkg_descriptor.get('ontology-version', oceanproteinportal.ontology.getLatestOntologyVersion())
    # Get the mappings between data templates and the ontology
    template_mappings = oceanproteinportal.ontology.getTemplateMappings()
    if (ontology_version not in template_mappings):
        raise Exception('Unknown Ontology version')
    # Set the package name and other required fields
    pkg_name = constructPackageName(submission_name=submission_name, version_number=version_number)
    pkg_descriptor['name'] = pkg_name
    pkg_descriptor['opp:shortName'] = submission_name
    pkg_descriptor['profile'] = 'data-package'

    # Get the file-specific config


    # Build the pkg
    package = Package(pkg_descriptor)
    # Protein data
    proteins = Resource({
      'profile': 'tabular-data-resource',
      'path': protein_data['filename'],
      'name': pkg_name + '-proteins',
      'odo-dt:dataType': { '@id': oceanproteinportal.ontology.getDataFileType(type='protein', ontology_version=ontology_version) }
    })
    # Infer the field types
    proteins.infer()
    logging.info('PROTEIN Data:')
    # Map any known field names to the ontology knowledge
    for index, field in proteins.descriptor['schema']['fields']:
        if (field['name'] in template_mappings[ontology_version]['protein']):
            mapping = template_mappings[ontology_version]['protein'][field['name']]
            proteins.descriptor['schema']['fields'][index]['rdfType'] = mapping['class']
            proteins.descriptor['schema']['fields'][index]['type'] = mapping['type']
            logging.info('- PROTEIN field: %s' % (field))
        else:
            logging.info('- PROTEIN: Skipping unknown field %s' % (field))
    # Add the protein data descriptor to the package
    package.add_resource(proteins.descriptor)
    logging.info('Added PROTEIN data.')

    # Protein FASTA data
    logging.info('Protein FASTA Data:')
    fasta = Resource({
      'profile': 'data-resource',
      'path': fasta_data['filename'],
      'name': pkg_name + '-fasta',
      'encoding': 'utf-8',
      'format': 'fasta',
      'mediatype': 'text/fasta',
      'odo-dt:dataType': { '@id': oceanproteinportal.ontology.getDataFileType(type='fasta', ontology_version=ontology_version) }
    })
    # Add the protein FASTA data descriptor to the package
    package.add_resource(fasta.descriptor)
    logging.info('Added protein FASTA data.')

    # Peptide data
    peptides = Resource({
      'profile': 'tabular-data-resource',
      'path': peptide_data['filename'],
      'name': pkg_name + '-peptides',
      'odo-dt:dataType': { '@id': oceanproteinportal.ontology.getDataFileType(type='peptide', ontology_version=ontology_version) }
    })
    # Infer the field types
    peptides.infer()
    logging.info('PEPTIDE Data:')
    # Map any known field names to the ontology knowledge
    for index, field in peptides.descriptor['schema']['fields']:
        if (field['name'] in template_mappings[ontology_version]['peptide']):
            mapping = template_mappings[ontology_version]['protein'][field['name']]
            peptides.descriptor['schema']['fields'][index]['rdfType'] = mapping['class']
            peptides.descriptor['schema']['fields'][index]['type'] = mapping['type']
            logging.info('- PEPTIDE field: %s' % (field))
        else:
            logging.info('- PEPTIDE: Skipping unknown field %s' % (field))
    # Add the protein data descriptor to the package
    package.add_resource(peptides.descriptor)
    logging.info('Added PEPTIDE data.')

    # Validate the package
    package.commit()
    try:
        valid = datapackage.validate(package.descriptor)
        logging.info('Valid data package: %s' % (valid))
    except exceptions.ValidationError as exception:
        logging.exception('Validation errors occurred')
        raise exception

    # Save the datapackage if valid or no validation check required.
    if no_require_validation or package.valid:
        dp_path = save_path + '/datapackage.json'
        package.save(dp_path)
        logging.info('Saved the data package: %s' % (dp_path))
        return dp_path

    return None

def constructPackageName(submission_name, version_number):
    """Construct a package name.

    A lowercase, alphanumeric string allowing '.', '-', '_'.
    See https://frictionlessdata.io/specs/data-package/#name
    """
    pkg_name = submission_name + '_v' + version_number
    pattern = re.compile('[._-\W_]+')
    return pattern.sub('_', pkg_name).lower()


def datapackageCruises(datapackage):
    """Get cruise information from datapackage"""
    if 'odo:hasDeployment' not in datapackage.descriptor:
        return None

    cruises = {}
    for cruise in datapackage.descriptor['odo:hasDeployment']:
        label = cruise.get('name')
        cruises[label] = {
          'label': label,
          'uri': cruise.get('uri', None)
        }
    return cruises

def getDatapackageOntologyVersion(datapackage):
    """Get the ontology version defined in the datapackage"""
    return datapackage.descriptor.get('ontology-version', oceanproteinportal.ontology.getLatestOntologyVersion())

def getDatapackageOntology(datapackage):
    """Get the ontology URI prefix defined in the datapackage"""
    global DATAPACKAGE_ONTOLOGY_KEY

    ontology_version = getDatapackageOntologyVersion(datapackage)
    template_mappings = oceanproteinportal.ontology.getTemplateMappings()

    if (ontology_version not in template_mappings):
        raise Exception('No ontology defined for: %s' % (ontology_version))
    if (DATAPACKAGE_ONTOLOGY_KEY not in template_mappings[ontology_version]):
        raise Exception('Mapping does not define an ontology for version : %s' % (ontology_version))

    return template_mappings[ontology_version][DATAPACKAGE_ONTOLOGY_KEY]

def findResource(datapackage, resource_type):
    """Find a specific resource by its ontology class"""
    # Get the Ontology Version
    ontology_version = oceanproteinportal.datapackage.getDatapackageOntologyVersion(datapackage)

    dataTypeId = None
    # Find the resource
    for resource in datapackage.resources:
        dataType = resource.descriptor.get('odo-dt:dataType', None)
        if dataType is not None:
            dataTypeId = dataType.get('@id', None)
            if dataTypeId == oceanproteinportal.ontology.getDataFileType(type=resource_type, ontology_version=ontology_version):
                return resource

    return None

def processFieldValue( value, descriptor, field_type):
    """Process a field's value"""

    if 'missingValues' in descriptor:
        for miss in descriptor['missingValues']:
            if value == miss:
              return None

    #Convert to correct ES data type
    if descriptor['type'] == 'number':
        return float(value)
    elif descriptor['type'] == 'integer':
        return int(value)
    return value

def processField(value, descriptor, field_type, delimiterField='opp:fieldValueDelimiter'):
    """Process a field"""
    if None is value:
        return value

    is_array_value = False
    values = [value]

    # Is the value an array of values?
    if delimiterField in descriptor:
        is_array_value = True
        values = value.split(descriptor[delimiterField])

    # Are there contraints that help process the data?
    if 'constraints' in descriptor:
        if 'pattern' in descriptor['constraints']:
            for idx, val in enumerate(values):
                regex = re.compile('^{0}$'.format(descriptor['constraints']['pattern']))
                match = regex.match(value)
                if match and match.lastindex is 1:
                    #### could handle VALUE PROCESSING here
                    values[idx] = processFieldValue(match.group(1), descriptor, field_type)
                else:
                    # must be null
                    values[idx] = None
    else:
        for idx,val in enumerate(values):
            values[idx] = processFieldValue(val, descriptor, field_type)

    # Return the value
    if is_array_value:
        return values
    else:
        return values[0]
