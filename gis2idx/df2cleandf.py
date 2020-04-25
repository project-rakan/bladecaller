import pickle
import io
import os
import pandas as pd
import logging

from util import (
    parseState,
    INPUT_CSV_LOCATION,
    CACHE_LOCATION,
    LOGMODE
)
from gis2df import (
    STEP_CACHE_LOCATION as GIS2DF_CACHE
)

STEP_CACHE_LOCATION = CACHE_LOCATION + "df2cleandf/"

def initializeCache():
    "Create the cache defined in util.py if it doesn't exist"
    if not os.path.isdir(STEP_CACHE_LOCATION):
        logging.debug(f"Creating {STEP_CACHE_LOCATION}")
        os.mkdir(STEP_CACHE_LOCATION)

def readLastArtifact(state: str):
    "Load the previous artifact into memory"
    with io.open(GIS2DF_CACHE + f'{state}.df.a.pk', 'rb') as handle:
        payload = pickle.loads(handle.read())
    return payload

def appendDemographicsData(state: str, df):
    demo = pd.read_csv(INPUT_CSV_LOCATION.format(state=state))

    # Add the relavent columns to the main dataframe (df)
    """
    *******      Key to P00300X labels:      ******
    P003001 => Total
    P003002 => White alone
    P003003 => Black or African American alone
    P003004 => American Indian and Alaska Native alone
    P003005 => Asian alone
    P003006 => Native Hawaiian and Other Pacific Islander alone
    P003007 => Some Other Race alone
    P003008 => Two or More Races
    """
    colsToAdd = ['GEOID', 'P003001', 'P003002', 'P003003', 'P003004',
                'P003005','P003006', 'P003007', 'P003008']
    readableNames = ['GEOID', 'TotalPop', 'WhitePop', 'BlackPop', 'NativeAPop',
                    'AsianPop', 'PacIsPop', 'OtherPop', 'MultiPop']

    demo = demo[colsToAdd] # Narrow down demographic data to the most relevant set
    demo = demo.rename(columns=dict(zip(colsToAdd, readableNames)))

    # Merge the demographic data with the main dataframe
    # Match df's types
    cols=[i for i in demo.columns if i not in ["GEOID"]]
    for col in cols:
        demo[col] = demo[col].astype(int)
    demo["GEOID"] = demo["GEOID"].astype(str)

    # Some GEOID's that begin with 0 get shortened
    # Re-add the leading 0 before merge
    if len(demo["GEOID"][0]) == 10:
        demo["GEOID"] = demo["GEOID"].map(lambda x: '0'+x)

    # Merge the dataframes
    df = pd.merge(df, demo, on="GEOID", how="left")

    # Check for missing data in the map data or demographic data
    # Demographic data that can't be placed on a map
    dataLeftOut = set(demo["GEOID"]) - set(df["GEOID"])
    if len(dataLeftOut) > 0:
        total = 0
        for geo in dataLeftOut:
            total += int(demo[demo["GEOID"] == list(dataLeftOut)[0]]["TotalPop"])
        logging.debug(f"Unable to merge data for {dataLeftOut} precincts ({total} population)")

    # Map data without any demographic data
    dataMissing = set(df["GEOID"]) - set(demo["GEOID"])
    if len(dataMissing) > 0:
        logging.debug(f"Unable to find data for {dataMissing} precincts")


    return df


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
    # flag any water-only precincts exist
    waterOnly = sum(df["ALAND"] == 0)
    logging.debug(f"{state} has {waterOnly} water-only precincts")
        
    # check for multipolygons
    multiPoly = sum(df["geometry"].geom_type == 'MultiPolygon')
    logging.debug(f"{state} has {multiPoly} precincts represented with multipolygons")

    logging.debug(f"Loading in the demographic data from {INPUT_CSV_LOCATION.format(state=state)}")
    df = appendDemographicsData(state, df)
    logging.debug(f"Finished merge of demographic data")

    # Save to cache
    initializeCache()
    df.to_csv(f".gis2idx_cache/df2cleandf/{state}.df.b.csv")


if __name__ == "__main__":
    logging.basicConfig(filename='df2cleandf.log', level=logging.DEBUG, filemode=LOGMODE)
    main()