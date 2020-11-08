"""This is meant to deal with coinfiguration parameters."""

import os
import json

DEFAULTS = dict()

DEFAULTS = {
    "DIR": r"__output__",
    "JSON": True,
    "SQLITE3": True,
    "PICKLE": True,
    "CSV": True
    }

# Try to read the configuration file. If it is not available use the DEFAULTS.
try:
    with open("config.json", 'r') as __file:
        config = json.load(__file)
        # Check if there are missing keys. If so, then will use the DEFAULTS to
        # fill the missing keys.
        missing = set(DEFAULTS.keys()) - set(config.keys())
        config = {**config, **{key: DEFAULTS[key] for key in missing}}
except FileNotFoundError:
    # TODO: Log it.
    config = DEFAULTS

# Create the output dir if it doesn't exists.
if not os.path.isdir(config["DIR"]):
    os.makedirs(config["DIR"])
