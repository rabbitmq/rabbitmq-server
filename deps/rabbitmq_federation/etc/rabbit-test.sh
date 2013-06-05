#!/bin/sh
CTL=$1

# Test direct connections
$CTL set_parameter federation-upstream localhost '{"uri": "amqp://"}'
# We will test the guest:guest gets stripped out in user_id_test
$CTL set_parameter federation-upstream local5673 '{"uri": "amqp://guest:guest@localhost:5673"}'

$CTL set_parameter federation-upstream-set upstream     '[{"upstream": "localhost", "exchange": "upstream", "queue": "upstream"}]'
$CTL set_parameter federation-upstream-set upstream2     '[{"upstream": "localhost", "exchange": "upstream2", "queue": "upstream2"}]'
$CTL set_parameter federation-upstream-set localhost    '[{"upstream": "localhost"}]'
$CTL set_parameter federation-upstream-set upstream12   '[{"upstream": "localhost", "exchange": "upstream", "queue": "upstream"},
                                                          {"upstream": "localhost", "exchange": "upstream2", "queue": "upstream2"}]'
$CTL set_parameter federation-upstream-set one          '[{"upstream": "localhost", "exchange": "one", "queue": "one"}]'
$CTL set_parameter federation-upstream-set two          '[{"upstream": "localhost", "exchange": "two", "queue": "two"}]'
$CTL set_parameter federation-upstream-set upstream5673 '[{"upstream": "local5673", "exchange": "upstream"}]'

$CTL set_policy fed   "^fed\."   '{"federation-upstream-set": "upstream"}'
$CTL set_policy fed12 "^fed12\." '{"federation-upstream-set": "upstream12"}'
$CTL set_policy one   "^two$"    '{"federation-upstream-set": "one"}'
$CTL set_policy two   "^one$"    '{"federation-upstream-set": "two"}'
$CTL set_policy hare  "^hare\."  '{"federation-upstream-set": "upstream5673"}'
$CTL set_policy all   "^all\."   '{"federation-upstream-set": "all"}'
$CTL set_policy new   "^new\."   '{"federation-upstream-set": "new-set"}'
