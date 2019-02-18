"""
Interact with theontology for the OceanProteinPortal.
"""

def getLatestOntologyVersion():
    return 'v1.0'

def getVocabularies():
    return {
        'opp': 'http://schema.oceanproteinportal.org/v1.0/',
        'odo': 'http://ocean-data.org/schema/',
        'odo-dt': 'http://ocean-data.org/schema/data-type/v1.0/'
    }

def getDataFileType(type):
    vocabs = getVocabularies()
    if (type == 'protein'):
        return vocabs['odo-dt'] + 'ProteinSpectralCounts'
    elif (type == 'fasta'):
        return vocabs['odo-dt'] + 'FASTA-ProteinIdentifications'
    elif (type == 'peptide'):
        return vocabs['odo-dt'] + 'PeptideSpectralCounts'
    return None

