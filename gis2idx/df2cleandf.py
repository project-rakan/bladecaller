import pickle
import io
import pandas as pd
from geopandas import GeoDataFrame
from shapely.geometry import Point
import fiona
import logging

from util import (
    parseState
)
from gis2df import (
    STEP_CACHE_LOCATION
)

def readLastArtifact(state: str):
    "Load the previous artifact into memory"
    with io.open(STEP_CACHE_LOCATION + f'{state}.df.a.pk', 'rb') as handle:
        payload = pickle.loads(handle.read())
    return payload
    

def main():
    "Clean up the dataframes for the state, checking for edge cases that need to be handled"

    # Get state
    logging.debug(f"Parsing state")
    state = parseState()
    logging.debug(f"Retrieved state {state}")

    logging.debug(f"Loading in the artifact: {state}.df.a.pk")
    df = readLastArtifact(state)
    logging.debug(f"Successfully loaded artifact")


    logging.debug(f"Checking for edge cases")
    #flag any water-only precincts exist
    waterOnly = sum(df["ALAND"] == 0)
    logging.debug(f"{state} has {waterOnly} water-only precincts")
        
    #check for multipolygons
    multiPoly = sum(df["geometry"].geom_type == 'MultiPolygon')
    logging.debug(f"{state} has {multiPoly} precincts represented with multipolygons")


if __name__ == "__main__":
    logging.basicConfig(filename='df2cleandf.log', level=logging.DEBUG)
    main()