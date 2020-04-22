# Project Bladecaller: The GIS -> IDX Pipeline 

This project will provide a set of data used for powering Rakan (backend), and visualization data for powering Xayah (frontend).

## Using Existing Artifacts

Since this script takes a bit to run, a set of artifacts have already been produced [here](https://github.com/project-rakan/bladecaller/tree/master/output).

## Replicating Results/Script Usage

While it's possible without docker, it's recommended users use it.

For an individual build (without using `docker-compose`, see [this script]() if you're interested in deploying the entire thing at once), perform the following stes:

0. Check that the state you want to convert is available. You can customize the demographic/voting data by changing the `csv` file.
1. Run `docker build . --tag bladecaller` in the same directory as the `Dockerfile` file.
2. Run `docker run bladecaller <state>` where `<state>` is one of the options listed in the repo.
3. The outputted `<state>.idx` and `<state>.json` will be saved as `data/output/<state>/<state>.idx` and `output/<state>/<state>.json` respectively.

## Workflow

A recommended strategy is working from a docker container. Run the command: `docker run -it --volume "<absolute path to current directory>:/home/project" --rm ubuntu:latest`
