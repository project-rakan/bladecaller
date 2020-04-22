"""
A set of utilities useful for this project.
"""
import os
from typing import AnyStr, List

from exceptions import (
    DirectoryNotFoundError,
    NoGISFilesFoundException,
    NoCSVFilesFoundException
)

INPUT_PREFIX = 'data/'
OUTPUT_PREFIX = 'output/'
INPUT_GIS_LOCATION = INPUT_PREFIX + '{state}/gis/'
INPUT_CSV_LOCATION = INPUT_PREFIX + '{state}/{state}.csv'
OUTPUT_IDX_LOCATION = OUTPUT_PREFIX + '{state}/{state}.idx'
OUTPUT_JSON_LOCATION = OUTPUT_PREFIX + '{state}/{state}.json'
CACHE_LOCATION = '.gis2idx_cache/'
MAGIC_NUMBER = "BEEFCAFE"

def generateCSVTemplate(state_name: AnyStr):
    """
        Takes in a statename, and produces a CSV template for filling out demographic data.
    """
    raise NotImplementedError()

def intToStrHex(integer: int, maxBytes: int = 4):
    "Convert an integer to a string representation of its hex equivalent without the 0x"
    strHex = str(hex(integer))[2:]
    if len(strHex) > (maxBytes * 2):
        raise ValueError(f"{integer} cannot be represented in {maxBytes} bytes")
    return "0" * ((2 * maxBytes) - len(str(strHex))) + str(strHex)

def parseState(arguments: List[str]):
    "Takes in a sys.argv command, extracts the state from it, and checks if it exists"

    if len(arguments) < 2:
        raise ValueError("No state specified")

    # Check if the directory containing states exist/output directory exists
    if not os.path.isdir(INPUT_PREFIX):
        raise DirectoryNotFoundError(f"Unable to state input directory: '{INPUT_PREFIX}'")

    state = arguments[1]

    if not os.path.isdir(INPUT_PREFIX + state):
        raise ValueError("State not found")

    return state
