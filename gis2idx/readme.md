# `gis2idx` pipeline

The idea behind the design of this tool, is to divide the pipeline into a series of steps, each consumes and produces intermediate artifacts. This allows for a pool of workers to complete whatever step needs to be done. The steps are described below.

## Step 0: Convert Shapefile to DataFrame
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