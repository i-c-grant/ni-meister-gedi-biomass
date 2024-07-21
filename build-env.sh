#!/bin/bash

# source activate base
basedir=$( cd "$(dirname "$0")" ; pwd -P)
echo installing environment...
conda env create --name nmbim-env -f ${basedir}/environment.yml

# Install the maap.py environment
echo trying to install maap-py...
source activate nmbim-env
git clone --single-branch --branch v3.1.4 https://github.com/MAAP-Project/maap-py.git
cd maap-py
pip install -e .
echo installed maap-py package!
