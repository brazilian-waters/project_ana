"""This module stores all constants used by the project"""

# Folder to store all data.
DATA_DIR = "data"

SYSTEMS_URL = {'sin': r"https://www.ana.gov.br/sar0/MedicaoSin",
               'nordestes_semiarido': r"https://www.ana.gov.br/sar0/Medicao",
               'cantareira': r"https://www.ana.gov.br/sar0/MedicaoCantareira"}
# URLs of each system. ANA's website shows reservoir data divided into 3 main 
# systems: 'SIN', 'Sistema Cantareira' and 'Nordeste e Semiarido'.