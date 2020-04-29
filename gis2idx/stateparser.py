import io
import os
import logging

import geopandas as geopd
import pandas as pd

import pickle

from typing import List
from util import (
    CACHE_LOCATION,
    INPUT_PREFIX,
    CACHE_LOCATION,
    STATEPARSER_CACHE_LOCATION,
    parseState
)

VTD_LOCATION = INPUT_PREFIX + '{state}/vtd/'
TRACTS_LOCATION = INPUT_PREFIX + '{state}/tracts/'
VOTES_LOCATION = INPUT_PREFIX + '{state}/votes/'
DEMOGRAPHIC_LOCATION = INPUT_PREFIX + '{state}/{state}.csv'

class State(object):

    def __init__(self, state: str, loadFromCache: bool = False):
        if not os.path.isdir(f'data/{state}'):
            raise ValueError(f"State {state} not found")

        self._state = state
        self._callStack: List[str] = []

        if loadFromCache:
            self.load()
       
    def loadVtd(self):
        "Load the VTD data into this state"
        df = geopd.read_file(VTD_LOCATION.format(state=self._state))
        for col2del in [
            'STATEFP10', # 2010 Census state Federal Information Processing Standards (FIPS) code
            'NAME10', # 2010 Census voting district name (numerical)
            'LSAD10', # Unknown, Contains the values "V1, V2, 00"
            'MTFCC10', # Unknown, always contains "G5240"
            'FUNCSTAT10', # Unknown, always contains N/S
        ]:
            del df[col2del]

        df.rename(columns={
            # Convert/Delete columns to more human friendly messages
            'GEOID10': 'GEOID', # Voting district identifier from 2010
            'VTDST10': 'vtd', # 2010 Census voting district code
            'COUNTYFP10': 'countyfp', # 2010 Census state Federal Information Processing Standards (FIPS) code
            'VTDI10': 'vtdi', # 2010 Census voting district indicator (A = Actual, P = Pseudo) (will filter out P)
            'NAMELSAD10': 'name', # 2010 Census name and the translated legal/statistical area description for voting district
            'ALAND10': 'land', # 2010 Census land area (square meters),
            'AWATER10': 'water', # 2010 Census water area (square meters),
            'INTPTLAT10': 'center_y', # Lattitude of precinct
            'INTPTLON10': 'center_x', # Longitude of precinct
            'geometry': 'geometry', # Shape Details of VTD
        }, inplace=True)

        self._vtd_df = df
        
    def loadTracts(self):
        "Load Census Tracts into this state"
        df = geopd.read_file(TRACTS_LOCATION.format(state=self._state))

        for col2del in [
            'STATEFP', # 2010 Census state Federal Information Processing Standards (FIPS) code
            'NAME', # 2010 Census voting district name (numerical)
            'LSAD', # Unknown, Contains the values "V1, V2, 00"
            'AFFGEOID', # Unknown, some variation of GEOID,
        ]:
            del df[col2del]

        df.rename(columns={
            'ALAND': 'land',
            'AWATER': 'water',
            'TRACTCE': 'tract',
            'COUNTYFP': 'countyfp', # 2010 Census state Federal Information Processing Standards (FIPS) code
        }, inplace=True)

        self._tract_df = df

    def loadDemographics(self):
        "Load the CSV of demographics"
        df = pd.read_csv(DEMOGRAPHIC_LOCATION.format(state=self._state))
        
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

        df = df[colsToAdd]

        # Merge the demographic data with the main dataframe
        # Match df's types
        cols = [i for i in df.columns if i not in ["GEOID"]]
        for col in cols:
            df[col] = df[col].astype(int)
        df["GEOID"] = df["GEOID"].astype(str)

        # Some GEOID's that begin with 0 get shortened
        # Re-add the leading 0 before merge
        if len(df["GEOID"][0]) == 10:
            df["GEOID"] = df["GEOID"].map(lambda x: '0'+x)

        df.rename(columns=dict(zip(colsToAdd, readableNames)), inplace=True)

        self._demographic_df = df

    def loadVotes(self):
        "Load the voter data into this state"
        pass

    def dropMultiPolygons(self):
        "Drop the multi polygons"
        pass

    def dissolveGranularity(self, level):
        "Dissolve into counties, cities, etc"
        pass

    def mergeTables(self):
        "Use PostGIS to merge all datasets into one df"
        # merge demographics + tracts
        self.save()
        abs_path = os.path.abspath(STATEPARSER_CACHE_LOCATION + self._state + '.state.pk')
        output_path = os.path.abspath(STATEPARSER_CACHE_LOCATION + self._state + '.demographics.pk')
        os.system(f"cd gis2idx/datamerger && python3.7 manage.py parse_census_df \"{abs_path}\" \"{output_path}\"")
        
        with io.open(output_path, 'rb') as handle:
            census_df = pickle.load(handle)
    
        self._demographic_df = pd.merge(census_df, self._vtd_df, right_on='GEOID', left_on='geoid', how='left')

        # Drop multi-polygons here
        self.dropMultiPolygons()
        self.dissolveGranularity("county") # Todo: change

        for column in [
            'center_y', 'center_x', 'vtdi', 'vtd', 'geoid', 'GEOID'
        ]:
            del self._demographic_df[column]
        
        

        self.save()

    def save(self):
        "Cache to a pickle"
        initializeCache()
        with io.open(STATEPARSER_CACHE_LOCATION + self._state + '.state.pk', 'wb') as handle:
            pickle.dump(self._demographic_df, handle)
            pickle.dump(self._vtd_df, handle)
            pickle.dump(self._tract_df, handle)

    def load(self):
        "Load from pickle"
        with io.open(STATEPARSER_CACHE_LOCATION + self._state + '.state.pk', 'rb') as handle:
            self._demographic_df = pickle.load(handle)
            self._vtd_df = pickle.load(handle)
            self._tract_df = pickle.load(handle)

    def __str__(self):
        return self._state

def initializeCache():
    "Create the cache defined in util.py if it doesn't exist"

    logging.debug("Initializing Cache")
    if not os.path.isdir(CACHE_LOCATION):
        logging.debug(f"Creating {CACHE_LOCATION}")
        os.mkdir(CACHE_LOCATION)
    if not os.path.isdir(STATEPARSER_CACHE_LOCATION):
        logging.debug(f"Creating {STATEPARSER_CACHE_LOCATION}")
        os.mkdir(STATEPARSER_CACHE_LOCATION)

def main():
    state = parseState()
    stateHandle = State(state)
    stateHandle.loadVtd()
    stateHandle.loadTracts()
    stateHandle.loadDemographics()
    stateHandle.loadVotes()
    stateHandle.mergeTables()


if __name__ == "__main__":
    logging.basicConfig(filename='stateparser.log', level=logging.DEBUG)
    main()