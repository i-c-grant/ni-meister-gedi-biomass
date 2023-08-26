#!/bin/bash

# source activate base
basedir=$( cd "$(dirname "$0")" ; pwd -P)
conda env list
echo installing environment...
conda env create -f ${basedir}/environment.yml