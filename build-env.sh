#!/bin/bash

# source activate base
basedir=$( cd "$(dirname "$0")" ; pwd -P)
echo installing environment...
conda env create --name nmbim-env -f ${basedir}/environment.yml

