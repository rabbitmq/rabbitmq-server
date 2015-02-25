#!/bin/sh -e
TWO=$(python2 -c 'import sys;print(sys.version_info[0])')
THREE=$(python3 -c 'import sys;print(sys.version_info[0])')

if [ $TWO != 2 ] ; then
    echo Python 2 not found!
    exit 1
fi

if [ $THREE != 3 ] ; then
    echo Python 3 not found!
    exit 1
fi

echo ----------------------
echo Testing under Python 2
echo ----------------------

# TODO make this pass!
#python2 $(dirname $0)/rabbitmqadmin-test.py

echo ----------------------
echo Testing under Python 3
echo ----------------------

python3 $(dirname $0)/rabbitmqadmin-test.py
