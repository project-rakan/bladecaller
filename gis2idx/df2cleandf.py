import pickle
import io
import pandas as pd
from geopandas import GeoDataFrame
from shapely.geometry import Point
import fiona

from util import (
    parseState
)
from gis2df import (
    STEP_CACHE_LOCATION
)

def readLastArtifact(state: str):
    with io.open(STEP_CACHE_LOCATION + f'{state}.df.a.pk', 'rb') as handle:
        payload = pickle.loads(handle.read())
    import pdb; pdb.set_trace()
    return payload
    

def main():
    state = parseState()
    df = readLastArtifact(state)
    #flag any water-only precincts exist

    #check for multipolygons


if __name__ == "__main__":
    main()