#!/bin/bash

# source activate base
basedir=$( cd "$(dirname "$0")" ; pwd -P)
echo installing environment...
mamba env create -f ${basedir}/environment.yml

# Install the maap.py environment
source activate osgeo-env
git clone --single-branch --branch v3.0.1 https://github.com/MAAP-Project/maap-py.git ${basedir}
cd ${basedir}/maap-py
pip install -e .
