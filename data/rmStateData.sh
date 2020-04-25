#!/bin/bash  
INPUT=data/stateKeys.csv
OLDIFS=$IFS
IFS=','
[ ! -f $INPUT ] && { echo "$INPUT file not found"; exit 99; }
while read State STUSAB Name STATENS
do
    if [ "$State" != "STATE" ]; then
        rm -r data/$Name
    fi
done < $INPUT
IFS=$OLDIFS