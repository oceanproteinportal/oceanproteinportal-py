import logging
import re
import string
import sys
import yaml
from datapackage import Package, Resource, validate, exceptions
"""
Create a Frictionlessdata Data Package for the OceanProteinPortal.
"""

def buildTabularPackage(config_file):
    """Build a tabular OPP DataPackage.

    See examples/sample-datapackage-confgi.yaml"""

    # Read the configuration
    with open(config_file, 'r') as yamlfile:
        config = yaml.load(yamlfile)

    submission_name = config.get('name', None)
    if submission_name is None:
        raiseException('Submission Name is required.')
    version_number = config.get('version', None)
    if version_number is None:
        raiseException('Version Number is required.')
    protein_data = config.get('protein')
    if protein_data is None:
        raiseException('Path to protein spectral counts is required.')
    if peptide_data is None:
        raiseException('Path to peptide spectral counts is required.')
    if fasta_data is None:
        raiseException('Path to protein FASTA is required.')

    # Return variables
    dp_path = None
    errors = None

    # Get the mappings between data templates and the ontology
    template_mappings = oceanproteinportal.ontology.getTemplateMappings()

    # Data provider tells us which ontology they used
    ontology_version = config.get('ontology-version', oceanproteinportal.ontology.getLatestOntologyVersion())
    if (ontology_version not in template_mappings)
        raise Exception('Unknown Ontology version')

    pkg_name = constructPackageName(submission_name=submission_name, version_number=version_number)
    package = Package({'name': pkg_name})

    # Protein data
    proteins = Resource({
      'profile': 'tabular-data-resource',
      'path': protein_data,
      'name': pkg_name + '-proteins',
      'odo-dt:dataType': { '@id': oceanproteinportal.ontology.getDataFileType('protein') }
    })
    # Infer the field types
    proteins.infer()
    # Map any known field names to the ontology knowledge
    for index, field in proteins.descriptor['schema']['fields']:
        if (field['name'] in template_mappings[ontology_version]['protein']):
            mapping = template_mappings[ontology_version]['protein'][field['name']]
            proteins.descriptor['schema']['fields'][index]['rdfType'] = mapping['class']
            proteins.descriptor['schema']['fields'][index]['type'] = mapping['type']
    # Add the protein data descriptor to the package
    package.add_resource(proteins.descriptor)

    # Protein FASTA data
    fasta = Resource({
      'profile': 'data-resource',
      'path': fasta_data,
      'name': pkg_name + '-fasta',
      'encoding': 'utf-8',
      'format': 'fasta',
      'mediatype': 'text/fasta',
      'odo-dt:dataType': { '@id': oceanproteinportal.ontology.getDataFileType('fasta') }
    })
    # Add the protein FASTA data descriptor to the package
    package.add_resource(fasta.descriptor)

    # Peptide data
    peptides = Resource({
      'profile': 'tabular-data-resource',
      'path': peptide_data,
      'name': pkg_name + '-peptides',
      'odo-dt:dataType': { '@id': oceanproteinportal.ontology.getDataFileType('peptide') }
    })
    # Infer the field types
    peptides.infer()
    # Map any known field names to the ontology knowledge
    for index, field in peptides.descriptor['schema']['fields']:
        if (field['name'] in template_mappings[ontology_version]['peptide']):
            mapping = template_mappings[ontology_version]['protein'][field['name']]
            peptides.descriptor['schema']['fields'][index]['rdfType'] = mapping['class']
            peptides.descriptor['schema']['fields'][index]['type'] = mapping['type']
    # Add the protein data descriptor to the package
    package.add_resource(peptides.descriptor)

    # Validate the package
    package.commit()
    try:
        valid = validate(package.descriptor)
    except exceptions.ValidationError as exception:
        errors = exception.errors:

    # Save the datapackage
    if no_require_validation or package.valid:
        dp_path = save_path + '/datapackage.json'
        package.save(dp_path)

    return dp_path, errors

def constructPackageName(submission_name, version_number):
    """Construct a package name.

    A lowercase, alphanumeric string allowing '.', '-', '_'.
    See https://frictionlessdata.io/specs/data-package/#name
    """
    pkg_name = submission_name + '_v' + version_number
    pattern = re.compile('[._-\W_]+')
    return pattern.sub('_', pkg_name).lower()


