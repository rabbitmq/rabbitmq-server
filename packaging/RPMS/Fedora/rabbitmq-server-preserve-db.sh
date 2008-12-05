#!/bin/bash

if [ "$1x" = "x" ]; then 
	echo "You haven't specified the initial location of the RabbitMQ database"
	exit 1
fi

if [ ! -d "$1" ]; then
	echo "The directory containing the RabbitMQ database doesn't exist"
	exit 1
fi

echo 
echo "The RabbitMQ database schema has changed."
echo "If your RabbitMQ database contains important data,"
echo "such as user accounts, durable exchanges and queues,"
echo "or persistent messages, then we recommend you contact"
echo "support@rabbitmq.com for assistance with the upgrade."
echo "The current RabbitMQ database will be moved to the"
echo "directory: "

CURRENT_MNESIA_DIR=$1
DATE=`date +'%d_%m_%Y'`
TMP_OLD_MNESIA_DIR=$CURRENT_MNESIA_DIR.$$.${DATE}
mv "$CURRENT_MNESIA_DIR" "$TMP_OLD_MNESIA_DIR"

echo "$TMP_OLD_MNESIA_DIR"
echo