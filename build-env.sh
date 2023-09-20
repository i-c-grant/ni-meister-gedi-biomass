#!/bin/bash

# source activate base
basedir=$( cd "$(dirname "$0")" ; pwd -P)
echo installing environment...
mamba env create --name osgeo-env -f ${basedir}/environment.yml
conda env list
# conda create --name osgeo-env --clone base
# source activate osgeo-env
# mamba install -c conda-forge scipy -y
# mamba install -c conda-forge h5py -y
# mamba install -c conda-forge geopandas -y

# Install the maap.py environment
echo trying to install maap-py...
source activate osgeo-env
git clone --single-branch --branch v3.0.1 https://github.com/MAAP-Project/maap-py.git
cd maap-py
pip install -e .
echo installed maap-py package!
