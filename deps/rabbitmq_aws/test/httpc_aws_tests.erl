-module(httpc_aws_tests).

-include_lib("eunit/include/eunit.hrl").

-include("httpc_aws.hrl").


endpoint_test_() ->
  [
    {"dynamodb service", fun() ->
      Expectation = "dynamodb.us-west-2.amazonaws.com",
      ?assertEqual(Expectation, httpc_aws:endpoint("us-west-2", "dynamodb"))
     end}
  ].
