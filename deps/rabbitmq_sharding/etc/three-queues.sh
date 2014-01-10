#!/bin/sh
CTL=$1

$CTL set_parameter shard-definition 3_shard '{"local-username": "guest", "shards-per-node": 3}'
$CTL set_policy 3_shard   "^three\."   '{"shard-definition": "3_shard"}'
