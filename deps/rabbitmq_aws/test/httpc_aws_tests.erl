-module(httpc_aws_tests).

-include_lib("eunit/include/eunit.hrl").

-include("httpc_aws.hrl").

init_test_() ->
  {foreach,
    fun() ->
      os:putenv("AWS_DEFAULT_REGION", "us-west-3"),
      meck:new(httpc_aws_config, [passthrough])
    end,
    fun(_) ->
      os:unsetenv("AWS_DEFAULT_REGION"),
      meck:unload(httpc_aws_config)
    end,
    [
      {"ok", fun() ->
        os:putenv("AWS_ACCESS_KEY_ID", "Sésame"),
        os:putenv("AWS_SECRET_ACCESS_KEY", "ouvre-toi"),
        Expectation = {ok,{state,"Sésame","ouvre-toi",undefined,undefined,"us-west-3", undefined}},
        ?assertEqual(Expectation, httpc_aws:init([]))
       end},
      {"error", fun() ->
        meck:expect(httpc_aws_config, credentials, fun() -> {error, test_result} end),
        Expectation = {ok,{state,undefined,undefined,undefined,undefined,"us-west-3",test_result}},
        ?assertEqual(Expectation, httpc_aws:init([])),
        meck:validate(httpc_aws_config)
       end}
    ]
  }.


terminate_test() ->
  ?assertEqual(ok, httpc_aws:terminate(foo, bar)).


code_change_test() ->
  ?assertEqual({ok, {state, denial}}, httpc_aws:code_change(foo, bar, {state, denial})).


endpoint_test_() ->
  [
    {"dynamodb service", fun() ->
      Expectation = "dynamodb.us-west-2.amazonaws.com",
      ?assertEqual(Expectation, httpc_aws:endpoint("us-west-2", "dynamodb"))
     end}
  ].

format_response_test_() ->
  [
    {"ok", fun() ->
      Response = {ok, {{"HTTP/1.1", 200, "Ok"}, [{"Content-Type", "text/xml"}], "<test>Value</test>"}},
      Expectation = {ok, {[{"Content-Type", "text/xml"}], [{"test", "Value"}]}},
      ?assertEqual(Expectation, httpc_aws:format_response(Response))
     end},
    {"error", fun() ->
      Response = {ok, {{"HTTP/1.1", 500, "Internal Server Error"}, [{"Content-Type", "text/xml"}], "<error>Boom</error>"}},
      Expectation = {error, "Internal Server Error", {[{"Content-Type", "text/xml"}], [{"error", "Boom"}]}},
      ?assertEqual(Expectation, httpc_aws:format_response(Response))
     end}
  ].

get_content_type_test_() ->
  [
    {"from headers caps", fun() ->
      Headers = [{"Content-Type", "text/xml"}],
      Expectation = {"text", "xml"},
      ?assertEqual(Expectation, httpc_aws:get_content_type(Headers))
     end},
    {"from headers lower", fun() ->
      Headers = [{"content-type", "text/xml"}],
      Expectation = {"text", "xml"},
      ?assertEqual(Expectation, httpc_aws:get_content_type(Headers))
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
