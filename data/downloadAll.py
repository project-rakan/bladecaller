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