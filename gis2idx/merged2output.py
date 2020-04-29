import pickle
import io
import struct
import pandas as pd
import logging
import os
from shapely.geometry import mapping

from util import (
    # Constants
    CACHE_LOCATION,
    OUTPUT_PREFIX,
    OUTPUT_IDX_LOCATION,
    OUTPUT_JSON_LOCATION,
    MAGIC_NUMBER,
    LOGMODE,

    # Functions
    parseState
)
# TODO: Get correct storage location,
# TODO: Confirm that artifact will be a pickle data dump
MERGED_DF_INPUT = CACHE_LOCATION + 'gis2df/{state}.df.a.pk'

# .idx data formats
"""
Key:
    h ->    short       -> 2 bytes
    i ->    int         -> 4 bytes
    I ->    uint        -> 4 bytes
    l ->    long        -> 4 or 8 bytes
    q ->    long long   -> 8 bytes
    d ->    double      -> 8 bytes
    > ->    big endian
    < ->    little endian
"""
ENDIAN = '>'
HEADER_F = ENDIAN + 'Iii'   # The first uint(I) is to preserve the MagicNumer's hex value
NODE_RECORD_F = ENDIAN + 'iii'
NODE_ID_F = ENDIAN + 'q'    # Different from diagram (original: 'i', is 8B, was 4B), ID must be long
                            # Must be a longlong(q) because GEOIDs >=10^10
VERTEX_F = ENDIAN + 'dd'    # Different from diagram (original: 'ii', is 16B, was 8B)
                            #Vertex coords must be double
NEIGHBOR_F = NODE_ID_F
DEMOGRAPHICS_F = ENDIAN + 'hhhhhh' # Different from diagram (original: 'iiiiii', is 12B, was 24B)
                            # Demographics differ from diagram


def readLastArtifact(state: str):
    "Load the previous artifact into memory"
    with io.open(MERGED_DF_INPUT.format(state=state), 'rb') as handle:
        payload = pickle.loads(handle.read())
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
    "Creates a new column 'NEIGHBORS' that stores a list of neighbors for each precinct"
    for index, row in df.iterrows():  
        neighbors = df[df.geometry.touches(row['geometry'])].GEOID.tolist()
        df.at[index, "NEIGHBORS"] = ", ".join(neighbors)     
    return df

def getPolyCoords(geo):
    "Returns a tuple of x,y coords from a POLYGON in the form ((x1,y1),...,(xn,yn))"
    coordsList = []
    for i in range(len(geo)):
        coordsList.append(mapping(geo[i])["coordinates"][0])
    return coordsList

def getVertexStructList(vertList):
    "Returns a list of byte structs that each contain a coordinate (x,y)"
    vertices = []
    for v in vertList:
        vertices.append(struct.pack(VERTEX_F, v[0], v[1]))
    return vertices

def getNeighborStructList(neighborsList):
    "Returns a list of byte structs that each contain a neighbor GEOID"
    neighbors = []
    for n in neighborsList:
        neighbors.append(struct.pack(NEIGHBOR_F, int(n)))
    return neighbors

def packDemograpchics(prec):
    "Returns a byte structs that each contains the demographic data for the precinct"
    #TODO Missing HispanicPop, added TotalPop
    #['TotalPop', 'BlackPop', 'NativeAPop', 'AsianPop', 'WhitePop', 'OtherPop']
    return struct.pack(DEMOGRAPHICS_F, prec['TotalPop'],
                                        prec['BlackPop'],
                                        prec['NativeAPop'],
                                        prec['AsianPop'],
                                        prec['WhitePop'],
                                        prec['OtherPop'])

def calcNodeSize(numV, numN):
    "Returns the size of the node record in bytes"
    IDSize = struct.calcsize(NODE_ID_F)
    vertSize = struct.calcsize(VERTEX_F) * numV
    neighSize = struct.calcsize(NODE_ID_F) * numN
    demoSize = struct.calcsize(DEMOGRAPHICS_F)
    IDSize = struct.calcsize(NODE_ID_F)
    return IDSize + vertSize + neighSize + demoSize

def calcCheckSum():
    "Calculates a checksum to be included in the data header"
    return 12 #TODO checksum

def toIdx(df, state: str):
    "Formats and outputs a .idx from the data in the dataframe"
    # Add lists of neighbors for each precinct to the dataframe
    df = getNeighbors(df)

    # Convert each precinct's POLYGON into a list of (x,y) coordinates
    coordLists = getPolyCoords(df.geometry)

    # Used to store records for printing later
    nodeRecords = []
    nodes = []

    # To keep track of position of node records, cumulative length of previous records
    # TODO: Confirm that nodePos is not from start of file, but start of node data
    nodePos = 0

    for index, precinct in df.iterrows():
        # Pack node #[index]'s data
        # node_id
        nodeID = struct.pack(NODE_ID_F, int(precinct.GEOID))

        # vertex #1 - vertex #n_3
        verticesPacked = getVertexStructList(coordLists[index])

        # neighbor_id #1 - neighbor_id #n_4
        neighbors = df.at[index, "NEIGHBORS"].split(", ")
        neighborsPacked = getNeighborStructList(neighbors)

        # demographics
        #demoPacked = packDemograpchics(precinct) #TODO: uncomment once demographic data works

        #data for node_record #[index]
        numVertices = len(coordLists[index])
        numNeighbors = len(neighbors)
        
        # Create Node Record
        nodeRecord = struct.pack(NODE_RECORD_F, numVertices, numNeighbors, nodePos)

        # Store packed data for later printing
        nodeRecords.append(nodeRecord)
        node = [nodeID] + verticesPacked + neighborsPacked #TODO + demoPacked
        nodes.append(node)

        # recalculate nodePos for next record
        nodePos = nodePos + calcNodeSize(numVertices, numNeighbors)
        #END OF FORLOOP

    # Create State Header
    checkSum = calcCheckSum()
    numNodes = len(df)
    header = struct.pack(HEADER_F, MAGIC_NUMBER, checkSum, numNodes)

    # Output Struct Byte data to idx file
    fout = open(OUTPUT_IDX_LOCATION.format(state=state), 'wb')
    total = 0
    total += fout.write(header)
    for record in nodeRecords:
        total += fout.write(record)
    for node in nodes:
        for data in node:
            total += fout.write(data)
    return total
    

def main():
    "Creates the output .idx and json files from the cleaned and merged dataframe"
    # Get state
    logging.debug(f"Parsing state")
    state = parseState()
    logging.debug(f"Retrieved state {state}")

    # Load in merged data
    logging.debug(f"Loading in the artifact: " + MERGED_DF_INPUT.format(state=state))
    df = readLastArtifact(state)
    logging.debug(f"Successfully loaded artifact")

    #initialize output directory
    initializeOutput(state)

    # Output to .idx file
    logging.debug(f"Writing to " + OUTPUT_IDX_LOCATION.format(state=state))
    written = toIdx(df, state)
    logging.debug(f"Finished writing {written} bytes to .idx file")


if __name__ == "__main__":
    logging.basicConfig(filename='merged2output.log', level=logging.DEBUG, filemode=LOGMODE)
    main()

