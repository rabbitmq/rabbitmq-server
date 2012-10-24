#!/bin/sh
CTL=$1

$CTL set_parameter federation-upstream hare '{"uri": "amqp://localhost:5673"}'
$CTL set_parameter federation-upstream-set upstream '[{"upstream": "hare", "exchange": "upstream"}]'
$CTL set_policy fed "^fed\." '{"federation-upstream-set": "upstream"}'
$CTL add_user hare-user hare-user || true # User may already exist
$CTL set_permissions hare-user '.*' '.*' '.*'
