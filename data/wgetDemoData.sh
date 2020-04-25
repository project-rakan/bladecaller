#!/bin/bash  
# Shell script to download all 2018 census tract shapedata,
# as well as all 2010 census precinct demographic data
INPUT=data/stateKeys.csv
OLDIFS=$IFS
IFS=','
[ ! -f $INPUT ] && { echo "$INPUT file not found"; exit 99; }
while read State STUSAB Name STATENS
do
    if [ "$State" != "STATE" ]; then
            echo "Name : $State"
            mkdir -p "data/$Name/gis"

            #Download race demographics by precinct for the state
            #Save the file as data/"STATE"/"STATE".csv
            # Where "State" is the name of the state, starting with a capitol letter
            wget -O - http://censusdata.ire.org/$State/all_140_in_$State.P3.csv | gunzip > data/$Name/$Name.csv

            # Download the census tract GIS files and unzip them
            # to data/"State"/gis
            # Where "State" is the name of the state, starting with a capitol letter
            wget -O data/$Name/temp.zip https://www2.census.gov/geo/tiger/GENZ2018/shp/cb_2016_"$State"_tract_500k.zip
            unzip data/$Name/temp.zip -d data/$Name/gis/
            rm data/$Name/temp.zip
    fi
done < $INPUT
IFS=$OLDIFS