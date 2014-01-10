#!/bin/sh
CTL=$1

$CTL set_parameter shard-definition my_shard '{"local-username": "guest", "shards-per-node": 4}'
$CTL set_policy my_shard   "^shard\."   '{"shard-definition": "my_shard"}'
