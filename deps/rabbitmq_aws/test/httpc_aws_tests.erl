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


maybe_decode_body_test_() ->
  [
    {"application/x-amz-json-1.0", fun() ->
      ContentType = {"application", "x-amz-json-1.0"},
      Body = "{\"test\": true}",
      Expectation = [{"test", true}],
      ?assertEqual(Expectation, httpc_aws:maybe_decode_body(ContentType, Body))
     end},
    {"application/json", fun() ->
      ContentType = {"application", "json"},
      Body = "{\"test\": true}",
      Expectation = [{"test", true}],
      ?assertEqual(Expectation, httpc_aws:maybe_decode_body(ContentType, Body))
     end},
    {"text/xml", fun() ->
      ContentType = {"text", "xml"},
      Body = "<test><node>value</node></test>",
      Expectation = [{"test", [{"node", "value"}]}],
      ?assertEqual(Expectation, httpc_aws:maybe_decode_body(ContentType, Body))
     end},
    {"text/html [unsupported]", fun() ->
      ContentType = {"text", "html"},
      Body = "<html><head></head><body></body></html>",
      ?assertEqual(Body, httpc_aws:maybe_decode_body(ContentType, Body))
     end}
  ].

parse_content_type_test_() ->
  [
    {"application/x-amz-json-1.0", fun() ->
      Expectation = {"application", "x-amz-json-1.0"},
      ?assertEqual(Expectation, httpc_aws:parse_content_type("application/x-amz-json-1.0"))
     end},
    {"application/xml", fun() ->
      Expectation = {"application", "xml"},
      ?assertEqual(Expectation, httpc_aws:parse_content_type("application/xml"))
     end},
    {"text/xml;charset=UTF-8", fun() ->
      Expectation = {"text", "xml"},
      ?assertEqual(Expectation, httpc_aws:parse_content_type("text/xml"))
     end}
  ].
