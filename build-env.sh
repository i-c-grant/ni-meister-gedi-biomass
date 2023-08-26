#!/bin/bash

# source activate base
basedir=$( cd "$(dirname "$0")" ; pwd -P)
conda create --name osgeo-env python=3.11
conda env list
# here we want the algorithm job build to fail
echo didnt break?
# conda env update --name base -f ${basedir}/environment.yml