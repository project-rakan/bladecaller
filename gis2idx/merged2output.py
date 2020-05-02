# Usage: python merged2output.py [state] [Options]
# 'state' is a lowercase name of a US State
# Options:
# -idx      -> create only the state's .idx file
# -json     -> create only the state's .json file
# -jnovert  -> create only the state's .novert.json file
# None      -> create all files (.idx, .json, .novert.json)

import pickle
import io
import json
import struct
import pandas as pd
import logging
import os
import sys
import hashlib
import time
import csv
from shapely.geometry import mapping

from util import (
    # Constants
    STATEPARSER_CACHE_LOCATION,
    OUTPUT_PREFIX,
    OUTPUT_IDX_LOCATION,
    OUTPUT_JSON_LOCATION,
    MAGIC_NUMBER,
    STATEKEY_LOCATION,
    LOGMODE,

    # Functions
    parseState
)
MERGED_DF_INPUT = STATEPARSER_CACHE_LOCATION + '{state}.state.pk'

# .idx data formats
"""
Key:
Used:
    > ->    big endian
    B ->    unsigned char   -> 1 byte
    I ->    unsigned int    -> 4 bytes
    Q ->    u-long long     -> 8 bytes
Others:
    h ->    short       -> 2 bytes
    i ->    int         -> 4 bytes
    l ->    long        -> 4 or 8 bytes
    q ->    long long   -> 8 bytes
    d ->    double      -> 8 bytes
    < ->    little endian
"""
ENDIAN = '>'
HEADER_F = ENDIAN + 'IQQBBII'           # 18 bytes
NODE_RECORD_F = ENDIAN + 'II'           # 8 bytes
NODE_ID_F = ENDIAN + 'I'                # 4 bytes
AREA_F = ENDIAN + 'I'                   # 4 bytes
NEIGHBOR_F = NODE_ID_F                  # 4 bytes
DEMOGRAPHICS_F = ENDIAN + 'IIIIII'      # 24 bytes

#Used to break up header for checksum calculation
HEADER1_F = ENDIAN + 'I' # Just magic num, checksum doesnt need reformatting packing               
HEADER2_F = ENDIAN + 'BBII'


def readLastArtifact(state: str):
    "Load the previous artifact into memory"
    with io.open(MERGED_DF_INPUT.format(state=state), 'rb') as handle:
        payload = pickle.load(handle)
    return payload

def initializeOutput(state):
    "Create the output directory defined in util.py if it doesn't exist"

    logging.debug("Initializing Output")
    if not os.path.isdir(OUTPUT_PREFIX):
        logging.debug(f"Creating {OUTPUT_PREFIX}")
        os.mkdir(OUTPUT_PREFIX)
    
    outloc = OUTPUT_PREFIX + f"{state}/"
    if not os.path.isdir(outloc):
        logging.debug(f"Creating {outloc}")
        os.mkdir(outloc)

def getNeighbors(df):
    "Returns a 2D list that stores a list of neighbors for each precinct"
    geo = df.geometry.tolist()
    
    neighbors = [[] for i in range(len(geo))]
    for i in range(len(geo)):
        for j in range(i):
            if geo[i].touches(geo[j]):
                neighbors[i].append(j)
                neighbors[j].append(i)
    
    return neighbors

def getPolyCoords(geo):
    "Returns a tuple of x,y coords from a POLYGON in the form ((x1,y1),...,(xn,yn))"
    coordsList = []
    for i in range(len(geo)):
        coordsList.append(mapping(geo[i])["coordinates"][0])
    return coordsList

#Unused
def getVertexStructList(vertList):
    "Returns a list of byte structs that each contain a coordinate (x,y)"
    vertices = []
    for v in vertList:
        vertices.append(struct.pack(VERTEX_F, float(v[0]), float(v[1])))
    return vertices

def getNeighborStructList(neighborsList):
    "Returns a list of byte structs that each contain a neighbor GEOID"
    neighbors = [struct.pack(NEIGHBOR_F, int(n)) for n in neighborsList]
    return neighbors

def packDemograpchics(prec):
    "Returns a byte structs that each contains the demographic data for the precinct"
    # Sum otherpop and add data in correct order
    otherpop = prec['otherPop'] + prec['pacisPop'] + prec['multiPop']
    return struct.pack(DEMOGRAPHICS_F, int(prec['totalPop']),
                                        int(prec['blackPop']),
                                        int(prec['nativeAPop']),
                                        int(prec['asianPop']),
                                        int(prec['whitePop']),
                                        int(otherpop))

def calcNodeSize(numN):
    "Returns the size of the node record in bytes"
    IDSize = struct.calcsize(NODE_ID_F)
    areaSize = struct.calcsize(AREA_F)
    neighborsSize = struct.calcsize(NEIGHBOR_F) * numN
    demoSize = struct.calcsize(DEMOGRAPHICS_F)
    return IDSize + areaSize + neighborsSize + demoSize

def calcCheckSum(state):
    "Calculates a checksum to be included in the data header"
    idxHash = hashlib.md5()
    with open(OUTPUT_IDX_LOCATION.format(state=state)+'.temp', 'rb') as idx:
        while True:
            data = idx.read(2**20)
            if not data:
                break
            idxHash.update(data)
    idx.close()
        
    return idxHash.digest()

def getStateMeta(state):
    state = state[:1].upper() + state[1:]

    stateKeys = csv.reader(open(STATEKEY_LOCATION))
    for row in stateKeys:
        if state == row[2]:
            return row[1], int(row[4]), int(row[0])

    return None

def getTimeDiff(start):
    return round(time.time()-start, 1)

def toIdx(df, state: str, stCode: str, numDistricts: int):
    "Formats and outputs a .idx from the data in the dataframe"
    # Get lists of neighbors for each precinct
    neighborsLists = getNeighbors(df)

    # Used to store records for printing later
    nodeRecords = []
    nodes = []

    # To keep track of position of node records, cumulative length of previous records
    nodePos = 0

    for index, precinct in df.iterrows():
        # Pack node #[index]'s data

        # Create and Store Node Record
        numNeighbors = len(neighborsLists[index])
        nodeRecord = struct.pack(NODE_RECORD_F, numNeighbors, nodePos)
        nodeRecords.append(nodeRecord)

        # Pack node data
        nodeID = struct.pack(NODE_ID_F, int(index))
        area = struct.pack(AREA_F, precinct.land + precinct.water)

        # neighbor_id #1 - neighbor_id #n_4h
        neighborsPacked = getNeighborStructList(neighborsLists[index])

        # demographics
        demoPacked = packDemograpchics(precinct)

        # Create and Store Node
        node = [nodeID] + neighborsPacked + [demoPacked]
        nodes.append(node)

        # recalculate nodePos for next record
        nodePos = nodePos + calcNodeSize(numNeighbors)
        #END OF FORLOOP

    # Create second half of State Header, missing magic_number, checksum
    numNodes = len(df)
    header2 = struct.pack(HEADER2_F, ord(stCode[0]), ord(stCode[1]), numNodes, numDistricts)

    # Output Struct Byte data to idx file
    tempOut = open(OUTPUT_IDX_LOCATION.format(state=state)+'.temp', 'wb')
    tempTotal = 0
    tempTotal += tempOut.write(header2)
    for record in nodeRecords:
        tempTotal += tempOut.write(record)
    for node in nodes: # will be list of entries (id, area, neighbor1, ..., demographics)
        for data in node:
            tempTotal += tempOut.write(data)
    tempOut.close()

    # Calculate and pack checksum
    md5 = calcCheckSum(state)
    checkSum = md5[-4:]
    header1 = struct.pack(HEADER1_F, MAGIC_NUMBER)

    # Write Out new header including magic_num and checksum
    idxOut = open(OUTPUT_IDX_LOCATION.format(state=state), 'wb')
    idxTotal = 0
    idxTotal += idxOut.write(header1)
    idxTotal += idxOut.write(checkSum)

    #write out everything else that was stored in temp
    with open(OUTPUT_IDX_LOCATION.format(state=state)+'.temp', 'rb') as tempIn:
        data = tempIn.read()
        idxTotal += idxOut.write(data)
    tempIn.close()

    # Remove temp
    os.remove(OUTPUT_IDX_LOCATION.format(state=state)+'.temp')
    
    return idxTotal

def toJSON(df, state: str, stCode: str, maxDistricts: int, fips: int, includeV=True):

    # Convert each precinct's POLYGON into a list of (x,y) coordinates
    coordLists = getPolyCoords(df.geometry)

    precincts = []
    for index, prec in df.iterrows():
        precName = prec['name']
        precID = index

        vertices = []
        if includeV:
            for v in coordLists[index]:
                coord = {
                    "lat": float(v[0]),
                    "lng": float(v[1])
                }
                vertices.append(coord)
        districtID = 0 #TODO: Hard coded 0 for missing district IDs

        precinctEntry = {
            "name": precName,
            "id": precID,
            "vertices": vertices,
            "district": districtID
        }
        precincts.append(precinctEntry)

    dictionary = {
        "state": stCode,
        "maxDistricts": maxDistricts,
        "fips": fips,
        "precincts": precincts
    }

    json_loc = OUTPUT_JSON_LOCATION.format(state=state)
    if not includeV:
        json_loc = json_loc[:-5]+'.novert.json'

    with open(json_loc, "w") as outfile:
        return outfile.write(json.dumps(dictionary, indent = 4))
        
    
def main(arg):
    "Creates the output .idx and json files from the cleaned and merged dataframe"
    startTime = time.time()

    # Get state
    logging.debug(f"Parsing state")
    state = parseState()
    # Get metadata
    stCode, numDistricts, fips= getStateMeta(state)
    logging.debug(f"Retrieved state {state}")

    # Load in merged data
    logging.debug(f"Loading in the artifact: " + MERGED_DF_INPUT.format(state=state))
    df = readLastArtifact(state)
    logging.debug(f"Successfully loaded artifact")

    #initialize output directory
    initializeOutput(state)

    # Output to .idx file
    if(arg == None or arg == '-idx'):
        logging.debug(f"Writing to " + OUTPUT_IDX_LOCATION.format(state=state))
        written = toIdx(df, state, stCode, numDistricts)
        logging.debug(f"Finished writing {written} bytes to {state}.idx")

    # Output to .JSON file
    if (arg == None or arg == '-json'):
        logging.debug(f"Writing to " + OUTPUT_JSON_LOCATION.format(state=state))
        written = toJSON(df, state, stCode, numDistricts, fips)
        logging.debug(f"Finished writing {written} bytes to {state}.json")

    if (arg == None or arg == '-jnovert'):
        # Chang where to write to
        logging.debug(f"Writing to " + OUTPUT_JSON_LOCATION.format(state=state)[:-5]+'.novert.json')
        written = toJSON(df, state, stCode, numDistricts, fips, False)
        logging.debug(f"Finished writing {written} bytes to {state}.novert.json")

    logging.debug(f"Finished writing output in {getTimeDiff(startTime)} seconds\n\n\n")


if __name__ == "__main__":
    logging.basicConfig(filename='merged2output.log', level=logging.DEBUG, filemode=LOGMODE)
    main(sys.argv[2] if len(sys.argv) >= 3 else None)

