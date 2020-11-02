"""This module stores all constants used by the project"""

import os

DEFAULTS = dict()

DEFAULTS = {
    "DIR": r"C:\Users\2swim\Documents\ANA\__output__",
    "JSON": True,
    "SQLITE3": False,
    "PICKLE": False,
    "CSV": False
    }

# TODO: Get from a file (.yaml) and use the defaults if it is not available.
config = DEFAULTS

if not os.path.isdir(config["DIR"]):
    os.makedirs(config["DIR"])
