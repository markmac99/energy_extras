#!/bin/bash

# Script to keep various energy measurements in InfluxDB up to date
# These are measurements that sometimes don't change for hours, leading to jumps
# in values that skew averages
# Copyright Mark McIntyre, 2026

source ~/.bashrc
here="$( cd "$(dirname "$0")" >/dev/null 2>&1 ; pwd -P )"
cd $here
source ~/miniconda3/etc/profile.d/conda.sh
conda activate openhabstuff

python ./energyExtras.py keepupdated Anker_E5000_Load_Power
python ./energyExtras.py keepupdated Anker_E5000_Battery_Power
python ./energyExtras.py keepupdated Enphase_Envoy_Battery_Watts
