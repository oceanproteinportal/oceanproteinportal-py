"""
Interact with the ontology for the OceanProteinPortal.
"""

def getLatestOntologyVersion():
    """Read ontology to get latest version, but for now encode here"""
    return 'v1.0'

def getDataFileType(type, ontology_version=None):
    """Read ontology to get data file types, but for now encode here"""
    if (ontology_version is None)
        ontology_version = getLatestOntologyVersion()

    if (ontology_version == "v1.0"):
        uri = 'http://ocean-data.org/schema/data-type/v1.0/'
        if (type == 'protein'):
            return uri + 'ProteinSpectralCounts'
        elif (type == 'fasta'):
            return uri + 'FASTA-ProteinIdentifications'
        elif (type == 'peptide'):
            return uri + 'PeptideSpectralCounts'
        return None

def getTemplateMappings(config_file='config/ontology_template_mappings.yaml'):
    """Read how the template columns map to the ontology.

    !!! Move this information to the ontology !!!
    """
    # Read the configuration
    with open(config_file, 'r') as yamlfile:
        mappings = yaml.load(yamlfile)
    return mappings

