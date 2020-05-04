"""
# Bladecaller

A command line based tool for converting CSV + US Census GIS files 
"""

import sys
import logging
import os

import stateparser
import merged2output

from exceptions import (
    DirectoryNotFoundError,
    NoGISFilesFoundException,
    NoCSVFilesFoundException
)

from util import (
    # Constants
    INPUT_PREFIX,
    OUTPUT_PREFIX,
    OUTPUT_IDX_LOCATION,
    OUTPUT_JSON_LOCATION,
)

from stateparser import (
    VTD_LOCATION,
    TRACTS_LOCATION,
    VOTES_LOCATION,
    DEMOGRAPHIC_LOCATION
)

from merged2output import (
    MERGED_DF_INPUT
)

# This is the default set of states that the method will run on
# For dev purposes only, TODO: remove this list
WORKING = ['iowa', 'washington']

def processState(state: str, args):
    """
        Converts a state directory (location of GIS and CSV file) and produces an .idx and .json file.
        Assumes the proper state GIS/CSV files exist
    """
    logging.info(f"Processing state: {state}")
    if '-use_cache' in args:
        logging.info(f"Attempting to use previously cached stateparser data..")
        if not os.path.exists(MERGED_DF_INPUT.format(state=state)):
            logging.info(f"Cannot find cached data for {state}")
            logging.info(f"Running stateparser({state})")
            stateparser.main(state)
        else:
            logging.info(f"Found cached data for {state}!")
    else:
        logging.info(f"Running stateparser({state})")
        stateparser.main(state)
    
    #-idx, -readable, -json, -novert, -all, or NONE, Documentation in merged2output.py
    # default merged2output args
    outputArgs = [state] # default merged2output args
    for arg in args:
        if arg.startswith('-') and arg != '-use_cache':
            outputArgs.append(arg)

    logging.info(f"Running merged2output({str(outputArgs)[1:-1]})")
    merged2output.main(outputArgs)
    
def sanityChecks(state: str):
    if not os.path.isdir(VTD_LOCATION.format(state=state)):
            logging.warning(f"VTD gis files not found for {state}")
            raise NoGISFilesFoundException()

    if not os.path.isdir(TRACTS_LOCATION.format(state=state)):
        logging.warning(f"Census tract gis files not found for {state}")
        raise NoGISFilesFoundException()

    if not os.path.isdir(VOTES_LOCATION.format(state=state)):
        logging.warning(f"Voting data gis files not found for {state}")
        raise NoGISFilesFoundException()

    if not os.path.isfile(DEMOGRAPHIC_LOCATION.format(state=state)):
        logging.warning(f"Demographics CSV file not found for {state}")
        raise NoCSVFilesFoundException()

    if os.path.isfile(OUTPUT_IDX_LOCATION.format(state=state)):
        logging.warning(f"Overwriting existing IDX file for {state}")

    if os.path.isfile(OUTPUT_JSON_LOCATION.format(state=state)):
        logging.warning(f"Overwriting existing JSON file for {state}")

def getArgs():
    args = []
    for arg in sys.argv:
        if arg.startswith('-'):
            args.append(arg)
    return set(args)

def checkDirectories():
    if not os.path.isdir(INPUT_PREFIX):
        raise DirectoryNotFoundError(f"Unable to find input directory: '{INPUT_PREFIX}'")

    if not os.path.isdir(OUTPUT_PREFIX):
        raise DirectoryNotFoundError(f"Unable to find output directory: '{OUTPUT_PREFIX}'")

def main():
    "Reads all states in data/ and produces their artifacts in the output file"

    # Check if the directory containing states exist/output directory exists
    checkDirectories()

    # Get args set
    args = getArgs()

    # Get state argument if it exists
    stateList = os.listdir(INPUT_PREFIX)
    if len(sys.argv) > 1:
        if sys.argv[1] in stateList:
            stateList = [sys.argv[1]]

    # Iterate through all input states
    for state in stateList:
        if '.' in state or '_' in state:
            continue

        logging.info(f"Found state {state}")

        # Sanity Checks for input data
        sanityChecks(state)

        # Then perform processing
        if state in WORKING:
            processState(state, args)

if __name__ == "__main__":
    # Quick sanity check before running huge process on each state
    if len(sys.argv) == 1 or sys.argv[1].startswith('-') :
        print('Are you sure you would like to process all states? [y/n] ')
        inp = input()
        if inp not in ['y', 'Y']:
            sys.exit()

    logging.basicConfig(filename='gis2idx.log',level=logging.INFO,filemode='w')
    main()