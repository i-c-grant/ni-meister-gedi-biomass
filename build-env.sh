#!/bin/bash

source activate base
basedir=$( cd "$(dirname "$0")" ; pwd -P)
conda install ${basedir}/environment.yml