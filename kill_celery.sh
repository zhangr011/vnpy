#!/bin/bash

PROGRAM_NAME=celery

# Kill old processes
echo "-------PROGRAM_NAME1--------"
ID=`ps -ef | grep $PROGRAM_NAME | grep -v "$0" | grep -v "grep" | grep "python" | awk '{print $2}'`
echo $ID
for id in $ID
do
kill -9 $id
echo "killed $id"
done
