#!/bin/sh
CTL=$1

$CTL set_parameter sharding-definition 3_shard '{"local-username": "guest", "shards-per-node": 3}'
$CTL set_policy 3_shard   "^three\."   '{"sharding-definition": "3_shard"}'

$CTL set_parameter sharding-definition 3_shard '{"local-username": "guest", "shards-per-node": 5}'
$CTL set_policy 3_shard   "^three\."   '{"sharding-definition": "3_shard"}'