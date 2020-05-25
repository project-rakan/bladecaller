# `gis2idx` pipeline

The idea behind the design of this tool, is to divide the pipeline into a series of steps, each consumes and produces intermediate artifacts. This allows for a pool of workers to complete whatever step needs to be done. The steps are described below.

## Step 0: Convert Shapefile to DataFrame
```
python gis2idx/gis2df.py <state>
```
- Consumes: 
    - GIS files (`.shx`, `.sha`, etc, files)
- Produces: 
    - A `dataframe` csv dump (`.df.a.csv` file)

## Step 1: Filter DataFrame of Water, Multi-Polygon Precincts, etc
- Consumes:
    - `dataframe` csv dump (`.df.a.csv` file)
- Produces:
    - `dataframe` csv dump (`.df.b.csv` file)

Removes all water from the `csv` file.

## Step 2: Append Demographic Data to Dataframe
- Consumes: 
    - A `networkx` pickle (`.nxa` file) 
    - file of demographic information (`.csv` file)
- Prdouces:
    - A `networkx` pickle (`.nxb` file)


Usage: py gis2idx [state] [options]

If no state is given
    - the pipeline will run for every state that has its own file under 'data/'

If no options are given:
    - The pipeline will run the stateparser on the given state(s)
    - The pipeline will run merged2output without any additional options
        -This will create the .idx, .json, .novert.json, .districts.json files

Parser options:
    - '-use_cache' will skip the state parsing step for states that have cached artifacts whenever possible
    - '-parse' will only run the stateparser step of the pipeline, caching the results

Output options: 
    (can take multiple arguments, will only produce the output defined by the arguments given)
    - '-idx'        create the .idx file
    - '-json'       create the .json file
    - '-novert'     create the .novert.json file
    - '-readable'   create the .idx.json and .idx files
    - '-districts'  create the .districts.json
    - '-shp'        create the shp directory and .shp file to visualize the map
    - '-all'        create all 6 file types