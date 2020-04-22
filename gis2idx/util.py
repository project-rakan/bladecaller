"""
A set of utilities useful for this project.
"""

from typing import AnyStr

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