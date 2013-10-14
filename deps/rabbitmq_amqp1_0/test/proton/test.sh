#!/bin/sh -e
DIR=`dirname $0`

echo Sending one message...
# Not localhost in case it resolves to IPv6 (proton will not work)
$DIR/proton -c amqp://guest:guest@127.0.0.1

echo Sending 10k messages...
$DIR/proton -c amqp://guest:guest@127.0.0.1 -q -n 10000

echo Done
