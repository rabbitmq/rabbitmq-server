#!/bin/sh
CTL=$1
USER="O=client,CN=$(hostname)"

# Test direct connections
$CTL add_user "$USER" ''
$CTL set_permissions -p / "$USER" ".*" ".*" ".*"
$CTL set_topic_permissions -p / "$USER" "amq.topic" "test-topic|test-retained-topic|.*topic.*" "test-topic|test-retained-topic|.*topic.*|last-will"
