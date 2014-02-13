#!/bin/sh
CTL=$1

$CTL set_parameter sharding-definition rkey '{"local-username": "guest", "shards-per-node": 2, "routing-key": "1234"}'
$CTL set_policy rkey-shard   "^rkey\."   '{"sharding-definition": "rkey"}'
