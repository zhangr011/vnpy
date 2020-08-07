#!/bin/bash

CONDA_HOME=~/anaconda3

############ Added by Huang Jianwei at 2018-04-03
# To solve the problem about Javascript runtime
export PATH=$PATH:/usr/local/bin
############ Ended

BASE_PATH=$(cd `dirname $0`; pwd)
echo $BASE_PATH
cd `dirname $0`

$CONDA_HOME/envs/py37/bin/python refill_future_renko.py 127.0.0.1 A99 1   1>logs/refill_history_A99.log 2>>logs/refill_error.log
$CONDA_HOME/envs/py37/bin/python refill_future_renko.py 127.0.0.1 AG99 1  1>logs/refill_history_AG99.log 2>>logs/refill_error.log
$CONDA_HOME/envs/py37/bin/python refill_future_renko.py 127.0.0.1 AL99 5  1>logs/refill_history_AL99.log 2>>logs/refill_error.log
$CONDA_HOME/envs/py37/bin/python refill_future_renko.py 127.0.0.1 AP99 1  1>logs/refill_history_AP99.log 2>>logs/refill_error.log
$CONDA_HOME/envs/py37/bin/python refill_future_renko.py 127.0.0.1 AU99 0.05 1>logs/refill_history_AU99.log 2>>logs/refill_error.log
$CONDA_HOME/envs/py37/bin/python refill_future_renko.py 127.0.0.1 B99 1   1>logs/refill_history_B99.log  2>>logs/refill_error.log
$CONDA_HOME/envs/py37/bin/python refill_future_renko.py 127.0.0.1 BB99 0.05 1>logs/refill_history_BB99.log 2>>logs/refill_error.log
$CONDA_HOME/envs/py37/bin/python refill_future_renko.py 127.0.0.1 BU99 2  1>logs/refill_history_BU99.log 2>>logs/refill_error.log
$CONDA_HOME/envs/py37/bin/python refill_future_renko.py 127.0.0.1 C99 1   1>logs/refill_history_C99.log 2>>logs/refill_error.log
$CONDA_HOME/envs/py37/bin/python refill_future_renko.py 127.0.0.1 CF99 5  1>logs/refill_history_CF99.log 2>>logs/refill_error.log
$CONDA_HOME/envs/py37/bin/python refill_future_renko.py 127.0.0.1 CJ99 5  1>logs/refill_history_CJ99.log 2>>logs/refill_error.log
$CONDA_HOME/envs/py37/bin/python refill_future_renko.py 127.0.0.1 CS99 1  1>logs/refill_history_CS99.log 2>>logs/refill_error.log
$CONDA_HOME/envs/py37/bin/python refill_future_renko.py 127.0.0.1 CU99 10 1>logs/refill_history_CU99.log 2>>logs/refill_error.log
$CONDA_HOME/envs/py37/bin/python refill_future_renko.py 127.0.0.1 CY99 5 1>logs/refill_history_CY99.log 2>>logs/refill_error.log
$CONDA_HOME/envs/py37/bin/python refill_future_renko.py 127.0.0.1 EG99 1 1>logs/refill_history_EG99.log 2>>logs/refill_error.log
$CONDA_HOME/envs/py37/bin/python refill_future_renko.py 127.0.0.1 FB99 0.05 1>logs/refill_history_FB99.log 2>>logs/refill_error.log
$CONDA_HOME/envs/py37/bin/python refill_future_renko.py 127.0.0.1 FG99 1 1>logs/refill_history_FG99.log 2>>logs/refill_error.log
$CONDA_HOME/envs/py37/bin/python refill_future_renko.py 127.0.0.1 FU99 1 1>logs/refill_history_FU99.log 2>>logs/refill_error.log
$CONDA_HOME/envs/py37/bin/python refill_future_renko.py 127.0.0.1 HC99 1 1>logs/refill_history_HC99.log 2>>logs/refill_error.log

$CONDA_HOME/envs/py37/bin/python refill_future_renko.py 127.0.0.1 I99 0.5   1>logs/refill_history_I99.log 2>>logs/refill_error.log
$CONDA_HOME/envs/py37/bin/python refill_future_renko.py 127.0.0.1 IC99 0.2  1>logs/refill_history_IC99.log 2>>logs/refill_error.log
$CONDA_HOME/envs/py37/bin/python refill_future_renko.py 127.0.0.1 IF99 0.2  1>logs/refill_history_IF99.log 2>>logs/refill_error.log
$CONDA_HOME/envs/py37/bin/python refill_future_renko.py 127.0.0.1 IH99 0.2  1>logs/refill_history_IH99.log 2>>logs/refill_error.log
$CONDA_HOME/envs/py37/bin/python refill_future_renko.py 127.0.0.1 J99 0.5   1>logs/refill_history_J99.log 2>>logs/refill_error.log
$CONDA_HOME/envs/py37/bin/python refill_future_renko.py 127.0.0.1 JD99 1 1>logs/refill_history_JD99.log 2>>logs/refill_error.log
$CONDA_HOME/envs/py37/bin/python refill_future_renko.py 127.0.0.1 JM99 0.5  1>logs/refill_history_JM99.log 2>>logs/refill_error.log
$CONDA_HOME/envs/py37/bin/python refill_future_renko.py 127.0.0.1 JR99 1 1>logs/refill_history_JR99.log 2>>logs/refill_error.log

$CONDA_HOME/envs/py37/bin/python refill_future_renko.py 127.0.0.1 L99 5  1>logs/refill_history_L99.log 2>>logs/refill_error.log
$CONDA_HOME/envs/py37/bin/python refill_future_renko.py 127.0.0.1 LR99 1 1>logs/refill_history_LR99.log 2>>logs/refill_error.log
$CONDA_HOME/envs/py37/bin/python refill_future_renko.py 127.0.0.1 M99 1  1>logs/refill_history_M99.log 2>>logs/refill_error.log
$CONDA_HOME/envs/py37/bin/python refill_future_renko.py 127.0.0.1 MA99 1 1>logs/refill_history_MA99.log 2>>logs/refill_error.log
$CONDA_HOME/envs/py37/bin/python refill_future_renko.py 127.0.0.1 NI99 10.0 1>logs/refill_history_NI99.log 2>>logs/refill_error.log
$CONDA_HOME/envs/py37/bin/python refill_future_renko.py 127.0.0.1 NR99 5 1>logs/refill_history_NR99.log 2>>logs/refill_error.log

$CONDA_HOME/envs/py37/bin/python refill_future_renko.py 127.0.0.1 OI99 1 1>logs/refill_history_OI99.log 2>>logs/refill_error.log
$CONDA_HOME/envs/py37/bin/python refill_future_renko.py 127.0.0.1 P99 2  1>logs/refill_history_P99.log 2>>logs/refill_error.log
$CONDA_HOME/envs/py37/bin/python refill_future_renko.py 127.0.0.1 PB99 5 1>logs/refill_history_PB99.log 2>>logs/refill_error.log
#$CONDA_HOME/envs/py37/bin/python refill_future_renko.py 127.0.0.1 PM99 1 1>logs/refill_history_PM99.log 2>>logs/refill_error.log
$CONDA_HOME/envs/py37/bin/python refill_future_renko.py 127.0.0.1 PP99 1 1>logs/refill_history_PP99.log 2>>logs/refill_error.log

$CONDA_HOME/envs/py37/bin/python refill_future_renko.py 127.0.0.1 RB99 1 1>logs/refill_history_RB99.log 2>>logs/refill_error.log
#$CONDA_HOME/envs/py37/bin/python refill_future_renko.py 127.0.0.1 RI99 1 1>logs/refill_history_RI99.log 2>>logs/refill_error.log
$CONDA_HOME/envs/py37/bin/python refill_future_renko.py 127.0.0.1 RM99 1 1>logs/refill_history_RM99.log 2>>logs/refill_error.log
#$CONDA_HOME/envs/py37/bin/python refill_future_renko.py 127.0.0.1 RR99 1 1>logs/refill_history_RR99.log 2>>logs/refill_error.log
$CONDA_HOME/envs/py37/bin/python refill_future_renko.py 127.0.0.1 RS99 1 1>logs/refill_history_RS99.log 2>>logs/refill_error.log
$CONDA_HOME/envs/py37/bin/python refill_future_renko.py 127.0.0.1 RU99 5 1>logs/refill_history_RU99.log 2>>logs/refill_error.log

$CONDA_HOME/envs/py37/bin/python refill_future_renko.py 127.0.0.1 SC99 0.1  1>logs/refill_history_SC99.log 2>>logs/refill_error.log
$CONDA_HOME/envs/py37/bin/python refill_future_renko.py 127.0.0.1 SF99 2 1>logs/refill_history_SF99.log 2>>logs/refill_error.log
$CONDA_HOME/envs/py37/bin/python refill_future_renko.py 127.0.0.1 SM99 2 1>logs/refill_history_SM99.log 2>>logs/refill_error.log
$CONDA_HOME/envs/py37/bin/python refill_future_renko.py 127.0.0.1 SN99 10.0 1>logs/refill_history_SN99.log 2>>logs/refill_error.log
$CONDA_HOME/envs/py37/bin/python refill_future_renko.py 127.0.0.1 SP99 2 1>logs/refill_history_SP99.log 2>>logs/refill_error.log
$CONDA_HOME/envs/py37/bin/python refill_future_renko.py 127.0.0.1 SS99 5  1>logs/refill_history_SS99.log 2>>logs/refill_error.log
$CONDA_HOME/envs/py37/bin/python refill_future_renko.py 127.0.0.1 SR99 1 1>logs/refill_history_SR99.log 2>>logs/refill_error.log
$CONDA_HOME/envs/py37/bin/python refill_future_renko.py 127.0.0.1 T99 0.005 1>logs/refill_history_T99.log 2>>logs/refill_error.log
$CONDA_HOME/envs/py37/bin/python refill_future_renko.py 127.0.0.1 TA99 2 1>logs/refill_history_TA99.log 2>>logs/refill_error.log
$CONDA_HOME/envs/py37/bin/python refill_future_renko.py 127.0.0.1 TF99 0.005 1>logs/refill_history_TF99.log 2>>logs/refill_error.log
$CONDA_HOME/envs/py37/bin/python refill_future_renko.py 127.0.0.1 TS99 0.005 1>logs/refill_history_TS99.log 2>>logs/refill_error.log
$CONDA_HOME/envs/py37/bin/python refill_future_renko.py 127.0.0.1 UR99 1 1>logs/refill_history_UR99.log 2>>logs/refill_error.log
$CONDA_HOME/envs/py37/bin/python refill_future_renko.py 127.0.0.1 V99 5  1>logs/refill_history_V99.log 2>>logs/refill_error.log
$CONDA_HOME/envs/py37/bin/python refill_future_renko.py 127.0.0.1 WH99 1 1>logs/refill_history_WH99.log 2>>logs/refill_error.log
$CONDA_HOME/envs/py37/bin/python refill_future_renko.py 127.0.0.1 WR99 1 1>logs/refill_history_WR99.log 2>>logs/refill_error.log
$CONDA_HOME/envs/py37/bin/python refill_future_renko.py 127.0.0.1 Y99 2  1>logs/refill_history_Y99.log 2>>logs/refill_error.log
$CONDA_HOME/envs/py37/bin/python refill_future_renko.py 127.0.0.1 ZC99 0.2  1>logs/refill_history_ZC99.log 2>>logs/refill_error.log
$CONDA_HOME/envs/py37/bin/python refill_future_renko.py 127.0.0.1 ZN99 5 1>logs/refill_history_ZN99.log 2>>logs/refill_error.log

