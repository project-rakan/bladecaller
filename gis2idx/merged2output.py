import pickle
import io
import pandas as pd
import logging
import struct
from shapely.geometry import mapping

from util import (
    # Constants
    CACHE_LOCATION,
    OUTPUT_IDX_LOCATION,
    OUTPUT_JSON_LOCATION,
    LOGMODE,

    # Functions
    parseState
)
#TODO: get correct storage location,
# confirm that artifact will be a pickle data dump
MERGED_DF_INPUT = CACHE_LOCATION + 'gis2df/_{state}.df.a.pk'
MAGIC_NUM = 12 #TODO

# .idx data formats
"""
Key:
    h -> short  -> 2 bytes
    i -> int    -> 4 bytes
    l -> long   -> 8 bytes
    d -> double -> 8 bytes
"""
HEADER_F = 'iii'
NODE_RECORD_F = 'iii'
NODE_ID_F = 'l'
VERTEX_F = 'dd'
NEIGHBOR_F = NODE_ID_F
DEMOGRAPHICS_F = 'hhhhhh'



def readLastArtifact(state: str):
    "Load the previous artifact into memory"
    with io.open(MERGED_DF_INPUT.format(state=state), 'rb') as handle:
        payload = pickle.loads(handle.read())
    return payload


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
    #TODO added Totalpop, missing HispanicPop
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
    return 12 #TODO

def toIdx(df, state: str):
    "Formats and outputs a .idx from the data in the dataframe"
    # Add lists of neighbors for each precinct to the dataframe
    df = getNeighbors(df)

    # Convert each precinct's POLYGON into a list of (x,y) coordinates
    coordLists = getPolyCoords(df.geometry)

    # To keep track of position of node records, cumulative length of previous records
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
        #demoPacked = packDemograpchics(precinct)

        #data for node_record #[index]
        numVertices = len(coordLists[index])
        numNeighbors = len(neighbors)
        
        # Create Node Record
        nodeRecord = struct.pack(NODE_RECORD_F, numVertices, numNeighbors, nodePos)

        # recalculate nodePos for next record
        nodePos = nodePos + calcNodeSize(numVertices, numNeighbors)

        # TODO store / print / pass to stream the Packed data:
        # nodeRecord,
        # nodeID, 
        # verticesPacked, 
        # neighborsPacked, 
        # demoPacked
        import pdb; pdb.set_trace()

    # Create State Header
    magicNumber = MAGIC_NUM
    checkSum = calcCheckSum()
    numNodes = len(df)
    header = struct.pack(HEADER_F, magicNumber, checksum, num_nodes)

    # TODO store / print / pass to stream the Packed data:
    # header

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

    toIdx(df, state)


if __name__ == "__main__":
    logging.basicConfig(filename='merged2output.log', level=logging.DEBUG, filemode=LOGMODE)
    main()

