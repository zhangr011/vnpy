#!/bin/bash

CONDA_HOME=~/anaconda3
#$CONDA_HOME/bin/conda deactivate
#$CONDA_HOME/bin/conda activate py37

############ Added by Huang Jianwei at 2018-04-03
# To solve the problem about Javascript runtime
export PATH=$PATH:/usr/local/bin
############ Ended

BASE_PATH=$(cd `dirname $0`; pwd)
echo $BASE_PATH
cd `dirname $0`
PROGRAM_NAME=./remove_expired_logs.py

# 移除三天前的所有日志
$CONDA_HOME/envs/py37/bin/python $PROGRAM_NAME prod/binance01 3
$CONDA_HOME/envs/py37/bin/python $PROGRAM_NAME prod/binance02 3
$CONDA_HOME/envs/py37/bin/python $PROGRAM_NAME prod/binance03 3
$CONDA_HOME/envs/py37/bin/python $PROGRAM_NAME prod/fund01 3



