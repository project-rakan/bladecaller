from django.contrib.gis.geos import GEOSGeometry
from django.core.management.base import BaseCommand
from blocks.models import VTDBlock, TractBlock
from progress.bar import IncrementalBar

import pandas as pd

import os
import io
import json
import pickle

class Command(BaseCommand):
    help = "Read the last location"

    def add_arguments(self, parser):
        parser.add_argument('filepath', type=str)
        parser.add_argument('output', type=str)

    def reset_table(self):
        VTDBlock.objects.all().delete()
        TractBlock.objects.all().delete()

    def handle(self, *args, **options):
        filepath = options['filepath']
        output = options['output']

        # Load up the dataframes
        with io.open(filepath, 'rb') as handle:
            demographic_df = pickle.load(handle)
            vtd_df = pickle.load(handle)
            tract_df = pickle.load(handle)

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

        # Merge the Census Tract Dataframes, and dump them into the postgis database
        TractBlock.objects.bulk_create((
            TractBlock(
                state=filepath.split('/')[-1].split('.')[0],
                geometry=GEOSGeometry(row['geometry'].to_wkt()),

                land=row['land'],
                water=row['water'],

                totalPop=row['TotalPop'],
                whitePop=row['WhitePop'],
                blackPop=row['BlackPop'],
                nativeAPop=row['NativeAPop'],
                asianPop=row['AsianPop'],
                pacisPop=row['PacIsPop'],
                otherPop=row['OtherPop'],
                multiPop=row['MultiPop'],
                
            ) for index, row in pd.merge(tract_df, demographic_df, on="GEOID", how="left").iterrows()
        ))

        table = {
            'geoid': [],
            'totalPop': [],
            'whitePop': [],
            'blackPop': [],
            'nativeAPop': [],
            'asianPop': [],
            'pacisPop': [],
            'otherPop': [],
            'multiPop': [],
        }

        # Leverage PostGIS to generate the intersection, apply the assumption 

        for vtdblock in VTDBlock.objects.all():
            related_tracts = TractBlock.objects.filter(geometry__bboverlaps=vtdblock.geometry)
            table['geoid'].append(vtdblock.geoid)
            table['totalPop'].append(0)
            table['whitePop'].append(0)
            table['blackPop'].append(0)
            table['nativeAPop'].append(0)
            table['asianPop'].append(0)
            table['pacisPop'].append(0)
            table['otherPop'].append(0)
            table['multiPop'].append(0)

            for related_tract in related_tracts:
                overlap = -vtdblock.geometry.union(related_tract.geometry).area + vtdblock.geometry.area + related_tract.geometry.area
                partition = (overlap / related_tract.geometry.area)
                table['totalPop'][-1] += partition * related_tract.totalPop
                table['whitePop'][-1] += partition * related_tract.whitePop
                table['blackPop'][-1] += partition * related_tract.blackPop
                table['nativeAPop'][-1] += partition * related_tract.nativeAPop
                table['asianPop'][-1] += partition * related_tract.asianPop
                table['pacisPop'][-1] += partition * related_tract.pacisPop
                table['otherPop'][-1] += partition * related_tract.otherPop
                table['multiPop'][-1] += partition * related_tract.multiPop

            table['totalPop'][-1] += round(table['totalPop'][-1])
            table['whitePop'][-1] += round(table['whitePop'][-1])
            table['blackPop'][-1] += round(table['blackPop'][-1])
            table['nativeAPop'][-1] += round(table['nativeAPop'][-1])
            table['asianPop'][-1] += round(table['asianPop'][-1])
            table['pacisPop'][-1] += round(table['pacisPop'][-1])
            table['otherPop'][-1] += round(table['otherPop'][-1])
            table['multiPop'][-1] += round(table['multiPop'][-1])

        output_df = pd.DataFrame(data=table)

        with io.open(output, 'wb') as handle:
            pickle.dump(output_df, handle)    


        