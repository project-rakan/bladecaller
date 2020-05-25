from django.contrib.gis.geos import GEOSGeometry
from django.core.management.base import BaseCommand
from blocks.models import VTDBlock, DistrictBlock
from progress.bar import IncrementalBar

import pandas as pd
import geopandas as gpd

import os
import io
import json
import pickle

class Command(BaseCommand):
    help = "Add a column to a dataframe, that describes the district the precinct is in"

    def add_arguments(self, parser):
        parser.add_argument('filepath', type=str)
        parser.add_argument('output', type=str)
        parser.add_argument('congress_path', type=str)

    def load_congressional_districts(self, filepath):
        DistrictBlock.objects.bulk_create((
            DistrictBlock(
                district_id=row['CD116FP'],
                geometry=GEOSGeometry(row['geometry'].to_wkt())
            ) for index, row in gpd.read_file(filepath).iterrows()
        ))


    def reset_table(self):
        VTDBlock.objects.all().delete()

    def handle(self, *args, **options):
        filepath = options['filepath']
        output = options['output']
        congress_path = options['congress_path']

        if len(DistrictBlock.objects.all()) == 0:
            self.load_congressional_districts(congress_path)

        # Load up the dataframes
        with io.open(filepath, 'rb') as handle:
            # We're reusing the save file, but this only requires the 2nd dataframe
            _ = pickle.load(handle) 
            vtd_df = pickle.load(handle)
            _ = pickle.load(handle)

        self.reset_table()

        # Dump the VTD dataframe into the postgis database
        VTDBlock.objects.bulk_create((
            VTDBlock(
                state=filepath.split('/')[-1].split('.')[0],
                geoid=row['GEOID'],
                geometry=GEOSGeometry(row['geometry'].to_wkt()),
                land=row['land'],
                water=row['water'],
            ) for index, row in vtd_df.iterrows()
        ))

        # Merge the Iterate through all districts and get a list of overlapping precincts for each one.

        tabledict = {}

        # Leverage PostGIS to generate the intersection
        for district in DistrictBlock.objects.all():
            related_tracts = VTDBlock.objects.filter(geometry__bboverlaps=district.geometry) # The magical fast function

            for vtd in related_tracts: #VTDBlock.objects.all():
                intersect = vtd.geometry.intersection(district.geometry).area
                
                if tabledict.__contains__(vtd.geoid):
                    if intersect > tabledict[vtd.geoid][1]:
                        tabledict[vtd.geoid] = (district.district_id, intersect)
                else:
                    tabledict[vtd.geoid] = (district.district_id, intersect)

        table = {
            'geoid': [],
            'district': []
        }

        for key in tabledict.keys():
            entry = tabledict[key][0]
            table['geoid'].append(key)
            table['district'].append(entry)

        output_df = pd.DataFrame(data=table).drop_duplicates(subset=['geoid'], keep='last').reset_index(drop=True)

        with io.open(output, 'wb') as handle:
            pickle.dump(output_df, handle)    


        