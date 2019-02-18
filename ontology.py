"""
Interact with the ontology for the OceanProteinPortal.
"""

def getLatestOntologyVersion():
    """Read ontology to get latest version, but for now encode here"""
    return 'v1.0'

def getDataFileType(type):
    """Read ontology to get data file types, but for now encode here"""
    vocabs = getVocabularies()
    if (type == 'protein'):
        return 'http://ocean-data.org/schema/data-type/v1.0/ProteinSpectralCounts'
    elif (type == 'fasta'):
        return 'http://ocean-data.org/schema/data-type/v1.0/FASTA-ProteinIdentifications'
    elif (type == 'peptide'):
        return 'http://ocean-data.org/schema/data-type/v1.0/PeptideSpectralCounts'
    return None

def getTemplateMappings(config_file='templates/ontology_mappings.yaml'):
    """Read how the template columns map to the ontology.

    !!! Move this information to the ontology !!!
    """
    # Read the configuration
    with open(config_file, 'r') as yamlfile:
        mappings = yaml.load(yamlfile)
    return mappings
