"""
# Step 0: Convert GIS files from US Census to a dataframe containing only precincts
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
    LOGMODE,
    parseState
)

STEP_CACHE_LOCATION = CACHE_LOCATION + 'gis2df/'

HEADER_MAP = {
    # Convert/Delete columns to more human friendly messages
    'GEOID10': 'GEOID', # Voting district identifier from 2010
    'VTDST10': 'vtd', # 2010 Census voting district code
    'STATEFP10': None, # 2010 Census state Federal Information Processing Standards (FIPS) code
    'COUNTYFP10': 'countyfp', # 2010 Census state Federal Information Processing Standards (FIPS) code
    'VTDI10': 'vtdi', # 2010 Census voting district indicator (A = Actual, P = Pseudo) (will filter out P)
    'NAME10': None, # 2010 Census voting district name (numerical)
    'NAMELSAD10': 'name', # 2010 Census name and the translated legal/statistical area description for voting district
    'LSAD10': None, # Unknown, Contains the values "V1, V2, 00"
    'MTFCC10': None, # Unknown, always contains "G5240"
    'FUNCSTAT10': None, # Unknown, always contains N/S
    'ALAND10': 'land', # 2010 Census land area (square meters),
    'AWATER10': 'water', # 2010 Census water area (square meters),
    'INTPTLAT10': 'center_y', # Lattitude of precinct
    'INTPTLON10': 'centery_x', # Longitude of precinct
    'geometry': 'geometry', # Shape Details of VTD
}

def initializeCache():
    "Create the cache defined in util.py if it doesn't exist"

    logging.debug("Initializing Cache")
    if not os.path.isdir(CACHE_LOCATION):
        logging.debug(f"Creating {CACHE_LOCATION}")
        os.mkdir(CACHE_LOCATION)
    if not os.path.isdir(STEP_CACHE_LOCATION):
        logging.debug(f"Creating {STEP_CACHE_LOCATION}")
        os.mkdir(STEP_CACHE_LOCATION)

def cleanColumns(raw_df):
    logging.debug("Cleaning up columns")
    clean_df = geopandas.geodataframe.GeoDataFrame()
    for (old_col, new_col) in HEADER_MAP.items():
        if new_col is not None:
            logging.debug(f"Renaming column {old_col} to {new_col}")
            clean_df[new_col] = raw_df[old_col]

    return clean_df

def removePsuedoPrecincts(raw_df):
    logging.debug("Removing Psuedo Precincts")
    psuedo_precincts = raw_df['vtdi'] == 'P'
    logging.debug(f"Found and Removed {psuedo_precincts.sum()} ({psuedo_precincts.mean() * 100}% of precincts) psuedo-precincts")
    clean_df = raw_df[~psuedo_precincts]

    del clean_df['vtdi']

    return clean_df.reset_index(drop=True)

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
    raw_df: GeoDataFrame = geopandas.read_file(INPUT_GIS_LOCATION.format(state=state))
    census_df = removePsuedoPrecincts(cleanColumns(raw_df))

    # Save to cache (and initialize if it wasn't done before)
    logging.debug(f"Creating Cache {state}")
    initializeCache()

    with io.open(STEP_CACHE_LOCATION + f'{state}.df.a.pk', 'wb') as handle:
        payload = pickle.dumps(census_df)
        logging.debug(f"Writing {len(payload)} characters to {STEP_CACHE_LOCATION + f'{state}.df.a.pk'}")
        handle.write(payload)

    logging.debug(f"Completed {state}")


if __name__ == "__main__":
    logging.basicConfig(filename='gis2df.log', level=logging.DEBUG, filemode=LOGMODE)
    main()