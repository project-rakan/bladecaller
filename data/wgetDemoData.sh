#!/bin/bash  
INPUT=data/stateKeys.csv
OLDIFS=$IFS
IFS=','
[ ! -f $INPUT ] && { echo "$INPUT file not found"; exit 99; }
while read State STUSAB Name STATENS
do
    if [ "$State" != "STATE" ]; then
        echo "Name : $State"
        mkdir -p "data/$Name/gis"
        wget -O - http://censusdata.ire.org/$State/all_140_in_$State.P3.csv | gunzip > data/$Name/$Name.csv
    fi
done < $INPUT
IFS=$OLDIFS