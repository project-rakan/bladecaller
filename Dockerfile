FROM ubuntu:latest

ENV DEBIAN_FRONTEND=noninteractive

# Prepare to install 3.7 + GDAL libraries
RUN apt-get update --fix-missing
RUN apt-get install -y software-properties-common apt-utils
RUN add-apt-repository ppa:ubuntugis/ppa
RUN add-apt-repository ppa:deadsnakes/ppa
RUN apt-get update --fix-missing

# Install the packages for GDAL
RUN apt-get install libgdal-dev gdal-bin -y
# Install the packages for python
RUN apt-get install python3.7 python3-pip python3.7-dev -y

# Install the database
RUN apt-get install postgresql-10 -y
RUN apt-get install libpq-dev postgis -y

# Install additional packages for scripts
RUN apt-get install wget unzip -y

# Copy repo requirements in and install them
COPY ./requirements.txt /app/requirements.txt
RUN python3.7 -m pip install -r /app/requirements.txt

WORKDIR /home/project

# Install database components
RUN ln -fs /usr/share/zoneinfo/America/Los_Angles /etc/localtime

# For when the app actually works
# ENTRYPOINT [ "bash ./docker-entrypoint.sh && bash" ] 
