"""This module stores all constants used by the project"""

import os

DATA_DIR = "data" # Folder to store all data.
# Subfolder to store reservoirs'data:
RESERVOIR_DIR = os.path.join(DATA_DIR, "reservoirs") 
# Subfolder to store systems' data:
SYSTEMS_DIR = os.path.join(DATA_DIR, "systems")