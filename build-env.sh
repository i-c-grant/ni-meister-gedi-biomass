#!/bin/bash

# source activate base
basedir=$( cd "$(dirname "$0")" ; pwd -P)
echo "Creating conda environment from ${basedir}/environment.yml"
conda env create --name "nmbim-env" -f ${basedir}/environment.yml
