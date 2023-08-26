#!/bin/bash

# source activate base
basedir=$( cd "$(dirname "$0")" ; pwd -P)
conda env create -f environment.yml