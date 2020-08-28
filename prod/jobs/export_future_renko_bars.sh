#!/bin/bash

CONDA_HOME=~/anaconda3

BASE_PATH=$(cd `dirname $0`; pwd)
echo $BASE_PATH
cd `dirname $0`
PROGRAM_NAME=./export_future_renko_bars.py

# 定时 mongodb => future_renko bars
$CONDA_HOME/envs/py37/bin/python $PROGRAM_NAME 127.0.0.1 FutureRenko



