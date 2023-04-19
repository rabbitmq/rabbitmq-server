-module(rabbitmq_aws_json_tests).

-include_lib("eunit/include/eunit.hrl").

-include("rabbitmq_aws.hrl").

parse_test_() ->
  [
    {"string decoding", fun() ->
      Value = "{\"requestId\":\"bda7fbdb-eddb-41fa-8626-7ba87923d690\",\"number\":128,\"enabled\":true,\"tagSet\":[{\"resourceId\":\"i-13a4abea\",\"resourceType\":\"instance\",\"key\":\"Environment\",\"value\":\"prod-us-east-1\"},{\"resourceId\":\"i-13a4abea\",\"resourceType\":\"instance\",\"key\":\"aws:cloudformation:logical-id\",\"value\":\"AutoScalingGroup\"},{\"resourceId\":\"i-13a4abea\",\"resourceType\":\"instance\",\"key\":\"aws:cloudformation:stack-name\",\"value\":\"prod-us-east-1-ecs-1\"}]}",
      Expectation = [
        {"requestId","bda7fbdb-eddb-41fa-8626-7ba87923d690"},
        {"number", 128},
        {"enabled", true},
        {"tagSet",
          [{"resourceId","i-13a4abea"},
            {"resourceType","instance"},
            {"key","Environment"},
            {"value","prod-us-east-1"},
            {"resourceId","i-13a4abea"},
            {"resourceType","instance"},
            {"key","aws:cloudformation:logical-id"},
            {"value","AutoScalingGroup"},
            {"resourceId","i-13a4abea"},
            {"resourceType","instance"},
            {"key","aws:cloudformation:stack-name"},
            {"value","prod-us-east-1-ecs-1"}]}
      ],
      Proplist = rabbitmq_aws_json:decode(Value),
      ?assertEqual(proplists:get_value("requestId", Expectation), proplists:get_value("requestId", Proplist)),
      ?assertEqual(proplists:get_value("number", Expectation), proplists:get_value("number", Proplist)),
      ?assertEqual(proplists:get_value("enabled", Expectation), proplists:get_value("enabled", Proplist)),
      ?assertEqual(lists:usort(proplists:get_value("tagSet", Expectation)),
                   lists:usort(proplists:get_value("tagSet", Proplist)))
     end},
    {"binary decoding", fun() ->
      Value = <<"{\"requestId\":\"bda7fbdb-eddb-41fa-8626-7ba87923d690\",\"number\":128,\"enabled\":true,\"tagSet\":[{\"resourceId\":\"i-13a4abea\",\"resourceType\":\"instance\",\"key\":\"Environment\",\"value\":\"prod-us-east-1\"},{\"resourceId\":\"i-13a4abea\",\"resourceType\":\"instance\",\"key\":\"aws:cloudformation:logical-id\",\"value\":\"AutoScalingGroup\"},{\"resourceId\":\"i-13a4abea\",\"resourceType\":\"instance\",\"key\":\"aws:cloudformation:stack-name\",\"value\":\"prod-us-east-1-ecs-1\"}]}">>,
      Expectation = [
        {"requestId","bda7fbdb-eddb-41fa-8626-7ba87923d690"},
        {"number", 128},
        {"enabled", true},
        {"tagSet",
          [{"resourceId","i-13a4abea"},
            {"resourceType","instance"},
            {"key","Environment"},
            {"value","prod-us-east-1"},
            {"resourceId","i-13a4abea"},
            {"resourceType","instance"},
            {"key","aws:cloudformation:logical-id"},
            {"value","AutoScalingGroup"},
            {"resourceId","i-13a4abea"},
            {"resourceType","instance"},
            {"key","aws:cloudformation:stack-name"},
            {"value","prod-us-east-1-ecs-1"}]}
      ],
      Proplist = rabbitmq_aws_json:decode(Value),
      ?assertEqual(proplists:get_value("requestId", Expectation), proplists:get_value("requestId", Proplist)),
      ?assertEqual(proplists:get_value("number", Expectation), proplists:get_value("number", Proplist)),
      ?assertEqual(proplists:get_value("enabled", Expectation), proplists:get_value("enabled", Proplist)),
      ?assertEqual(lists:usort(proplists:get_value("tagSet", Expectation)),
                   lists:usort(proplists:get_value("tagSet", Proplist)))
     end},
    {"list values", fun() ->
      Value = "{\"misc\": [\"foo\", true, 123]\}",
      Expectation = [{"misc", ["foo", true, 123]}],
      ?assertEqual(Expectation, rabbitmq_aws_json:decode(Value))
     end},
    {"empty objects", fun() ->
      Value = "{\"tags\": [{}]}",
      Expectation = [{"tags", [{}]}],
      ?assertEqual(Expectation, rabbitmq_aws_json:decode(Value))
      end}
  ].
