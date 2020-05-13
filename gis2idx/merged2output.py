# Usage: python merged2output.py [state] [Options]
# 'state' is a lowercase name of a US State
# Options:
# None      -> create the .idx, .json, .novert.json files
# -idx      -> create the state's .idx file
# -json     -> create the state's .json file
# -novert  -> create the state's .novert.json file
# -readable -> create a .idx.json that contains the data that is encoded in the .idx
#                (will also recreate the .idx file)
# -all      -> create all 4 file types


import pickle
import io
import json
import struct
import pandas as pd
import geopandas
import logging
import os
import sys
import hashlib
import time
import csv
import zlib
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
SHP_OUTPUT = OUTPUT_PREFIX + '{state}/shp/'

ARGUMENTS = set(['-idx', '-json', '-novert', '-readable', '-districts', '-shp'])

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
HEADER1_F = ENDIAN + 'II' # Just magic num, checksum doesnt need reformatting packing               
HEADER2_F = ENDIAN + 'BBII'


def readLastArtifact(state: str):
    "Load the previous artifact into memory"
    with io.open(MERGED_DF_INPUT.format(state=state), 'rb') as handle:
        payload = pickle.load(handle)
    return payload

def initializeOutput(state):
    "Create the output directory defined in util.py if it doesn't exist"

    logging.info("Initializing Output")
    if not os.path.isdir(OUTPUT_PREFIX):
        logging.info(f"Creating {OUTPUT_PREFIX}")
        os.mkdir(OUTPUT_PREFIX)
    
    outloc = OUTPUT_PREFIX + f"{state}/"
    if not os.path.isdir(outloc):
        logging.info(f"Creating {outloc}")
        os.mkdir(outloc)

def getNeighbors(df):
    "Returns a 2D list that stores a list of neighbors for each precinct"
    geo = df['geometry'].tolist()
    
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
    packed = struct.pack(DEMOGRAPHICS_F, int(prec['totalPop']),
                                        int(prec['blackPop']),
                                        int(prec['nativeAPop']),
                                        int(prec['asianPop']),
                                        int(prec['whitePop']),
                                        int(otherpop))
    readable = [int(prec['totalPop']),
                int(prec['blackPop']),
                int(prec['nativeAPop']),
                int(prec['asianPop']),
                int(prec['whitePop']),
                int(otherpop)]

    return packed, readable

def calcNodeSize(numN):
    "Returns the size of the node record in bytes"
    IDSize = struct.calcsize(NODE_ID_F)
    areaSize = struct.calcsize(AREA_F)
    neighborsSize = struct.calcsize(NEIGHBOR_F) * numN
    demoSize = struct.calcsize(DEMOGRAPHICS_F)
    return IDSize + areaSize + neighborsSize + demoSize

def calcCheckSum(state):
    "Calculates a checksum to be included in the data header"
    prev = 0
    with open(OUTPUT_IDX_LOCATION.format(state=state)+'.temp', 'rb') as idx:
        while True:
            data = idx.read(2**20)
            if not data:
                break
            prev = zlib.crc32(data, prev)

    idx.close()

    return prev

def getStateMeta(state):
    state = state[:1].upper() + state[1:]

    stateKeys = csv.reader(open(STATEKEY_LOCATION))
    for row in stateKeys:
        if state == row[2]:
            return row[1], int(row[4]), int(row[0])

    return None

def getTimeDiff(start):
    return round(time.time()-start, 1)

def toIdx(df, state: str, stCode: str, numDistricts: int, readable=False):
    "Formats and outputs a .idx from the data in the dataframe"
    # Get lists of neighbors for each precinct
    neighborsLists = getNeighbors(df)

    # Used to store records for printing later
    nodeRecords = []
    nodes = []

    # In case the user wants readable output for testing
    readableRecs = []
    readableNodes = []

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
        demoPacked, readableDemo = packDemograpchics(precinct)

        # Create and Store Node
        node = [nodeID] + [area] + neighborsPacked + [demoPacked]
        nodes.append(node)

        

        if (readable):
            readableRecs.append((int(index), numNeighbors, nodePos))
            readableNodes.append((int(index), 
                                precinct.land + precinct.water,
                                neighborsLists[index],
                                readableDemo))

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
    checkSum = calcCheckSum(state)
    header1 = struct.pack(HEADER1_F, MAGIC_NUMBER, checkSum)

    # Write Out new header including magic_num and checksum
    idxOut = open(OUTPUT_IDX_LOCATION.format(state=state), 'wb')
    idxTotal = 0
    idxTotal += idxOut.write(header1)

    #write out everything else that was stored in temp
    with open(OUTPUT_IDX_LOCATION.format(state=state)+'.temp', 'rb') as tempIn:
        data = tempIn.read()
        idxTotal += idxOut.write(data)
    tempIn.close()

    logging.info(f"Finished writing {idxTotal} bytes to {state}.idx")

    # Remove temp
    os.remove(OUTPUT_IDX_LOCATION.format(state=state)+'.temp')

    # print readable .idx.json
    if(readable):
        logging.info(f"Writing to " + OUTPUT_IDX_LOCATION.format(state=state) + '.json')
        written = readableIDX(state, checkSum, stCode, numNodes, numDistricts, readableRecs, readableNodes)
        logging.info(f"Finished writing {written} bytes to {state}.idx.json")
    

def readableIDX(state, checkSum, stCode, numNodes, numDistricts, nodeRecords, nodesList):
    records = []
    for rec in nodeRecords:
        record = {
            "nodeID": rec[0],
            "numNeighbors": rec[1],
            "nodePos": rec[2]
        }
        records.append(record)
    nodes = []
    for n in nodesList:
        node = {
            "nodeID": n[0],
            "area": n[1],
            "neighbors": n[2],
            "demographics": {
                "totalPop": n[3][0],
                "blackPop": n[3][1],
                "nativeAPop": n[3][2],
                "asianPoP": n[3][3],
                "whitePop": n[3][4],
                "otherPop": n[3][5]
            }
        }
        nodes.append(node)
    header = {
        "magic_num": hex(MAGIC_NUMBER),
        "checkSum": hex(checkSum),
        "stCode": stCode,
        "numNodes": numNodes,
        "numDistricts": numDistricts,
        "node_records": records,
        "nodes": nodes
    }

    with open(OUTPUT_IDX_LOCATION.format(state=state)+'.json', "w") as outfile:
        return outfile.write(json.dumps(header, indent = 4))


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
                    "lat": float(v[1]),
                    "lng": float(v[0])
                }
                vertices.append(coord)

        precinctEntry = {
            "name": precName,
            "id": precID,
            "vertices": vertices,
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

def checkArgs(args) :
    "Checks that all arguments passed in are valid, returns only the valid ones in a set"
    clean = []
    if args == None or len(args) == 0:
        return None
    for arg in args:
        if arg == '-all':
            return set([arg])
        if arg not in ARGUMENTS:
            print("Unknown argument: " + arg)
        else:
            clean.append(arg)
    if len(clean) == 0:
        return None
    return set(clean) 

def toJSONDict(df, state, stCode):
    mapping = []
    for index, prec in df.iterrows():
        # mapping.append(
        #     {"Precinct:": int(index), "District": prec['district']}
        # )
        mapping.append([int(index), prec['district']])
    output = {
        "state": stCode,
        "map": mapping
    }
    districtsLoc = OUTPUT_JSON_LOCATION.format(state=state)[:-5]+'.districts.json'
    with open(districtsLoc, "w") as outfile:
        return outfile.write(json.dumps(output, indent = 4))


def toSHP(df, state):
    shpDir = SHP_OUTPUT.format(state=state)
    if not os.path.isdir(shpDir):
        logging.info(f"Creating {shpDir}")
        os.mkdir(shpDir)
    geodf = geopandas.GeoDataFrame(df, geometry='geometry')
    geodf.to_file((SHP_OUTPUT + '{state}.shp').format(state=state))
        
    
def main(args):
    "Creates the output .idx and json files from the cleaned and merged dataframe"
    startTime = time.time()

    # Get state
    state = args[0]
    logging.info(f"Outputing data for state: " + state)
    

    # Check args
    args = checkArgs(set(args[1:]))

    # Get metadata
    stCode, numDistricts, fips= getStateMeta(state)
    logging.info(f"Retrieved state {state}")

    # Load in merged data
    logging.info(f"Loading in the artifact: " + MERGED_DF_INPUT.format(state=state))
    df = readLastArtifact(state)
    logging.info(f"Successfully loaded artifact")

    #initialize output directory
    initializeOutput(state)

    # Output to .shp file
    if (args != None and ('-shp' in args or '-all' in args)):
        toSHP(df, state)

    # Output to .idx file
    if (args != None and ('-all' in args or '-readable' in args)):
        logging.info(f"Writing to " + OUTPUT_IDX_LOCATION.format(state=state))
        written = toIdx(df, state, stCode, numDistricts, True)
    elif (args == None or '-idx' in args):
        logging.info(f"Writing to " + OUTPUT_IDX_LOCATION.format(state=state))
        written = toIdx(df, state, stCode, numDistricts)
    

    # Output to .JSON file
    if (args == None or '-json' in args or '-all' in args):
        logging.info(f"Writing to " + OUTPUT_JSON_LOCATION.format(state=state))
        written = toJSON(df, state, stCode, numDistricts, fips)
        logging.info(f"Finished writing {written} bytes to {state}.json")

    if (args == None or '-novert' in args or '-all' in args):
        # Chang where to write to
        logging.info(f"Writing to " + OUTPUT_JSON_LOCATION.format(state=state)[:-5]+'.novert.json')
        written = toJSON(df, state, stCode, numDistricts, fips, False)
        logging.info(f"Finished writing {written} bytes to {state}.novert.json")

    if (args == None or '-districts' in args or '-all in args'):
        logging.info(f"Writing to " + OUTPUT_JSON_LOCATION.format(state=state)[:-5]+'.districts.json')
        written = toJSONDict(df, state, stCode)
        logging.info(f"Finished writing {written} bytes to {state}.districts.json")

    logging.info(f"Finished writing {state} output in {getTimeDiff(startTime)} seconds\n\n")


if __name__ == "__main__":
    logging.basicConfig(filename='merged2output.log', level=logging.INFO, filemode=LOGMODE)
    main(sys.argv[1:] if len(sys.argv) >= 2 else None)

