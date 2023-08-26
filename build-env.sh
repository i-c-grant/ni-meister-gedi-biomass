#!/bin/bash

source activate base
basedir=$( cd "$(dirname "$0")" ; pwd -P)
conda env update --name base -f ${basedir}/environment.yml