FROM ubuntu:latest

# Prepare to install 3.7 + GDAL libraries
RUN apt-get update --fix-missing
RUN apt-get install -y software-properties-common apt-utils
RUN add-apt-repository ppa:ubuntugis/ppa
RUN add-apt-repository ppa:deadsnakes/ppa
RUN apt-get update

# Install the correct packages + pip
RUN apt-get install libgdal-dev gdal-bin -y
RUN apt-get install python3.7 python3-pip -y

# Copy repo requirements in and install them
COPY ./requirements.txt /app/requirements.txt
WORKDIR /app/
RUN python3.7 -m pip install -r requirements.txt

WORKDIR /home/project

# For when the app actually works
# ENTRYPOINT [ "python gis2idx/" ] 
