# Data Sources

All GIS files are pulled from the [US Census website](https://www.census.gov/geographies/mapping-files/time-series/geo/carto-boundary-file.html). Since the website format and its contents changes quite frequently, the data sources are pulled here. Note these are the precincts shape files from **2018** Census Tract datasets.

All demographic related (non-voter) data are pulled from the [IRE Census Bulk Data Website](http://census.ire.org/data/bulkdata.html). Note the demographic data comes from the **2010** Census.

All voter related data points are pulled from the [Harvard Dataverse](https://dataverse.harvard.edu/dataset.xhtml?persistentId=doi:10.7910/DVN/NH5S2I). Note that the **2016 presidential election data** was used. Thus, the assumption that voters who voted for a president of one party, voted for congressional representatives of the same party.

## Automation Notes
Due to the unreliability of the US Census website and it's ever changing nature, webscraping is difficult (which is why everything is posted here in the first place). At the time of this writing (April 2020), the following script was use to automatically download all GIS/CSV files for processing. The `stateKeys.csv` is left in the directory for future use. Note the `csv` file excludes states with only one district.

```py
# Originally written by Kyle McCulloh
# Ported to Python by Norton Pengra
import io
import os

def main():
    with io.open("stateKeys.csv") as handle:
        lines = handle.readlines()

    for line in lines:
        line = line.strip()
        fips, state_code, state_name, state_ns = line.split(',')

        if not os.path.isdir(state_name):
            print(f"Creating Directory for {state_name}")
            state_name = state_name.lower().replace(' ', '_')
            os.mkdir(f"{state_name}")
        else:
            print(f"Directory already exists for {state_name}")

        if not os.path.isfile(f"{state_name}/{state_name}.csv"):
            print(f"Downloading 2010 {state_name.title()} Census Data")
            os.system(f"wget -O - http://censusdata.ire.org/{fips}/all_140_in_{fips}.P3.csv | gunzip > {state_name}/{state_name}.csv")


        if not os.path.isdir(f"{state_name}/vtd/"):
            os.mkdir(f"{state_name}/vtd")
            print(f"Downloading 2010 {state_name.title()} Census VTD shapefiles")
            os.system(f"wget -O {state_name}/temp.zip https://www2.census.gov/geo/tiger/TIGER2012/VTD/tl_2012_{fips}_vtd10.zip")
            os.system(f"unzip {state_name}/temp.zip -d {state_name}/vtd/")
            os.system(f"rm {state_name}/temp.zip")

        if not os.path.isdir(f"{state_name}/tracts/"):
            os.mkdir(f"{state_name}/tracts")
            print(f"Downloading 2018 {state_name.title()} Census Tract shapefiles")
            os.system(f"wget -O {state_name}/temp.zip https://www2.census.gov/geo/tiger/GENZ2018/shp/cb_2018_{fips}_tract_500k.zip")
            os.system(f"unzip {state_name}/temp.zip -d {state_name}/tracts/")
            os.system(f"rm {state_name}/temp.zip")

        if not os.path.isdir(f"{state_name}/votes"):
            os.mkdir(f"{state_name}/votes")

if __name__ == "__main__":
    main()
```
        

# Notation/File Format

## GIS Files
As noted in the [US Census](https://www2.census.gov/geo/tiger/GENZ2018/2018_file_name_def.pdf), the following attributes are of paticular interest:

- `GEOID`: The US Census primary key, assigned to each US Census tract
- `STATEFP`: An integer identifier of a state
- `COUNTYFP`: An integer identifier of a county
- `NAME`: A string representing the human readable name of the region
- `ALAND`: An integer rerpesenting the area of land in the region
- `AWATER`: An intenger representing the area of water in the region

## CSV Files
As noted in the [US Census Documentation](https://raw.githubusercontent.com/ireapps/census/master/tools/metadata/sf1_labels.csv), the columns are of paticular interest:

- `GEOID`: The US Census primary key, assigned to each US Census tract
- `P003001`: Total population in precinct
- `P003002`: White population
- `P003003`: Black or African American population
- `P003004`: American Indian and Alaska Native population
- `P003005`: Asian population
- `P003006`: Native Hawaiian and Other Pacific Islander population
- `P003007`: Some Other Race population
- `P003008`: Two or More Races population 

# State Files

**Please note if you add your own states, the state name must be lowercase.**

## [Iowa]() (99 precincts, 4 districts)
*Note: [Iowa State constitution](http://publications.iowa.gov/135/1/history/7-7.html) Article III, Section 37 mandates that the districts respect county boundaries.*

- [GIS](https://github.com/project-rakan/bladecaller/tree/master/data/iowa/gis)
- [CSV](https://github.com/project-rakan/bladecaller/blob/master/data/iowa/iowa.csv)

## [Washington]() (99 precincts, 4 districts)
*Note: [Iowa State constitution](http://publications.iowa.gov/135/1/history/7-7.html) Article III, Section 37 mandates that the districts respect county boundaries. Thus a map of counties was generated by dissolving along the COUNTYFP field.*

- [GIS](https://github.com/project-rakan/bladecaller/tree/master/data/washington/gis)
- [CSV](https://github.com/project-rakan/bladecaller/blob/master/data/washington/washington.csv)

