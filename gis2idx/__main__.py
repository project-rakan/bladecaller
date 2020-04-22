"""
# Bladecaller

A command line based tool for converting CSV + US Census GIS files 
"""

import io
import os
import json
import geopandas
import logging
import binascii
import pandas as pd

from typing import IO, Any
from geopandas.geodataframe import GeoDataFrame

from exceptions import (
    DirectoryNotFoundError,
    NoGISFilesFoundException,
    NoCSVFilesFoundException
)

from util import (
    # Constants
    INPUT_PREFIX,
    OUTPUT_PREFIX,
    INPUT_CSV_LOCATION, 
    INPUT_GIS_LOCATION,
    OUTPUT_IDX_LOCATION,
    OUTPUT_JSON_LOCATION,
    CACHE_LOCATION,
    MAGIC_NUMBER,

    # Functions
    intToStrHex
)

def processState(state: str):
    """
        Converts a state directory (location of GIS and CSV file) and produces an .idx and .json file.
        Assumes the proper state GIS/CSV files exist
    """
    logging.debug(f"Processing state: {state}")

    # US Census GIS Files
    census_df: GeoDataFrame = geopandas.read_file(INPUT_GIS_LOCATION.format(state=state))

    # Our compiled research
    demographic_df: pd.DataFrame = pd.read_csv(INPUT_CSV_LOCATION.format(state=state))

    # Create our output directory if it doesn't exist
    if not os.path.isdir(f"{OUTPUT_PREFIX}{state}"):
        os.mkdir(f"{OUTPUT_PREFIX}{state}")
    
    # Create the IDX file
    createIDXFile(OUTPUT_IDX_LOCATION.format(state=state))

    raise NotImplementedError("Magic Happens Here")

    # Clean the water out
    # Create edges


def createJSONFile(location: str):
    with io.open(location) as handle:
        pass


def writeHeader(handle: IO[Any], magicHeader: str, checksum: int, numberOfNodes: int):
    "Write the magic number at the beginning of the file."

    # Convert to a string hex checksum (add 0s to the front of the 
    # checksum until there are 4 bytes)
    # Need to check when the size exceeds 4 bytes
    handle.seek(0)
    handle.write(binascii.unhexlify(magicHeader))
    handle.write(binascii.unhexlify(intToStrHex(checksum, maxBytes=4)))
    handle.write(binascii.unhexlify(intToStrHex(numberOfNodes, maxBytes=4)))


def createIDXFile(location: str):
    "Export an IDX file to the location specified"
    with io.open(location, 'wb') as handle:
        # Fill the header with dummy data for now
        writeHeader(handle, '0' * len(MAGIC_NUMBER), 0, 0)

        # TODO: Follow format defined in spec

        # Once everything has been written, write the real header
        writeHeader(handle, MAGIC_NUMBER, 0, 0)


def main():
    "Reads all states in data/ and produces their artifacts in the output file"

    # Check if the directory containing states exist/output directory exists
    if not os.path.isdir(INPUT_PREFIX):
        raise DirectoryNotFoundError(f"Unable to find input directory: '{INPUT_PREFIX}'")

    if not os.path.isdir(OUTPUT_PREFIX):
        raise DirectoryNotFoundError(f"Unable to find output directory: '{OUTPUT_PREFIX}'")

    # Iterate through all input states
    for state in os.listdir(INPUT_PREFIX):
        logging.debug(f"Found state {state}")
        # Perform a few sanity checks

        if not os.path.isdir(INPUT_GIS_LOCATION.format(state=state)):
            logging.warning(f"GIS files not found for {state}")
            raise NoGISFilesFoundException()

        if not os.path.isfile(INPUT_CSV_LOCATION.format(state=state)):
            logging.warning(f"CSV file not found for {state}")
            raise NoCSVFilesFoundException()

        if os.path.isfile(OUTPUT_IDX_LOCATION.format(state=state)):
            logging.warning(f"Overwriting existing IDX file for {state}")

        if os.path.isfile(OUTPUT_JSON_LOCATION.format(state=state)):
            logging.warning(f"Overwriting existing JSON file for {state}")

        # Then perform processing
        processState(state)

if __name__ == "__main__":
    logging.basicConfig(filename='gis2idx.log',level=logging.DEBUG)
    main()