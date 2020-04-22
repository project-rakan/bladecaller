"""
# Step 0: Convert GIS files from US Census to dataframe
"""
import pickle
import io
import os
import logging
import geopandas
from typing import IO, Any
from geopandas.geodataframe import GeoDataFrame

from exceptions import (
    DirectoryNotFoundError,
    NoGISFilesFoundException,
)
from util import (
    INPUT_GIS_LOCATION,
    CACHE_LOCATION,
    parseState
)

STEP_CACHE_LOCATION = CACHE_LOCATION + 'gis2df/'

def initializeCache():
    "Create the cache defined in util.py if it doesn't exist"

    logging.debug("Initializing Cache")
    if not os.path.isdir(CACHE_LOCATION):
        logging.debug(f"Creating {CACHE_LOCATION}")
        os.mkdir(CACHE_LOCATION)
    if not os.path.isdir(STEP_CACHE_LOCATION):
        logging.debug(f"Creating {STEP_CACHE_LOCATION}")
        os.mkdir(STEP_CACHE_LOCATION)

def main():
    "Convert the .shx/.sha files in the state data folder to dataframes and save it to disk"

    # Get state
    logging.debug(f"Parsing state")
    state = parseState()
    logging.debug(f"Retrieved state {state}")

    # Check GIS files exist and get them
    logging.debug(f"Check for GIS files in {state}")
    if not os.path.isdir(INPUT_GIS_LOCATION.format(state=state)):
        logging.warning(f"GIS files not found for {state}")
        raise NoGISFilesFoundException()

    logging.debug(f"Opening GIS files for {state}")
    census_df: GeoDataFrame = geopandas.read_file(INPUT_GIS_LOCATION.format(state=state))

    # Save to cache (and initialize if it wasn't done before)
    logging.debug(f"Creating Cache {state}")
    initializeCache()
    with io.open(STEP_CACHE_LOCATION + f'{state}.df.a.pk', 'wb') as handle:
        payload = pickle.dumps(census_df)
        logging.debug(f"Writing {len(payload)} characters to {STEP_CACHE_LOCATION + f'{state}.df.a.pk'}")
        handle.write(payload)

    logging.debug(f"Completed {state}")


if __name__ == "__main__":
    logging.basicConfig(filename='gis2df.log', level=logging.DEBUG)
    main()