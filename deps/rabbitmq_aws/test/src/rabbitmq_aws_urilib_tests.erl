-module(rabbitmq_aws_urilib_tests).

-include_lib("eunit/include/eunit.hrl").

-include("rabbitmq_aws.hrl").

build_test_() ->
  [
    {"variation1", fun() ->
      Expect = "amqp://guest:password@rabbitmq:5672/%2F?heartbeat=5",
      Value = #uri{scheme=amqp,
                   authority={{"guest", "password"}, "rabbitmq", 5672},
                   path="/%2F", query=[{"heartbeat", "5"}]},
      Result = rabbitmq_aws_urilib:build(Value),
      ?assertEqual(Expect, Result)
     end},
    {"variation2", fun() ->
      Expect = "http://www.google.com/search?foo=bar#baz",
      Value = #uri{scheme=http,
                   authority={{undefined, undefined}, "www.google.com", 80},
                   path="/search",
                   query=[{"foo", "bar"}],
                   fragment="#baz"},
      Result = rabbitmq_aws_urilib:build(Value),
      ?assertEqual(Expect, Result)
     end},
    {"variation3", fun() ->
      Expect = "https://www.google.com/search",
      Value = #uri{scheme=https,
                   authority={{undefined, undefined}, "www.google.com", undefined},
                   path="/search"},
      Result = rabbitmq_aws_urilib:build(Value),
      ?assertEqual(Expect, Result)
     end},
    {"variation4", fun() ->
      Expect = "https://www.google.com/search?foo",
      Value = #uri{scheme=https,
                   authority={{undefined, undefined}, "www.google.com", undefined},
                   path="/search",
                   query=["foo"]},
      Result = rabbitmq_aws_urilib:build(Value),
      ?assertEqual(Expect, Result)
     end},
    {"variation5", fun() ->
      Expect = "https://www.google.com/search?foo",
      Value = #uri{scheme=https,
                   authority={{undefined, undefined}, "www.google.com", 443},
                   path="/search",
                   query=["foo"]},
      Result = rabbitmq_aws_urilib:build(Value),
      ?assertEqual(Expect, Result)
     end},
    {"variation6", fun() ->
      Expect = "https://bar@www.google.com/search?foo",
      Value = #uri{scheme=https,
                   authority={{"bar", undefined}, "www.google.com", 443},
                   path="/search",
                   query=["foo"]},
      Result = rabbitmq_aws_urilib:build(Value),
      ?assertEqual(Expect, Result)
     end},
    {"variation7", fun() ->
      Expect = "https://www.google.com/search?foo",
      Value = #uri{scheme=https,
                   authority={undefined, "www.google.com", 443},
                   path="/search",
                   query=["foo"]},
      Result = rabbitmq_aws_urilib:build(Value),
      ?assertEqual(Expect, Result)
     end},
    {"variation8", fun() ->
      Expect = "https://www.google.com/search?foo",
      Value = #uri{scheme=https,
                   authority={{"", ""}, "www.google.com", 443},
                   path="/search",
                   query=["foo"]},
      Result = rabbitmq_aws_urilib:build(Value),
      ?assertEqual(Expect, Result)
     end},
    {"variation9", fun() ->
      Expect = "https://bar@www.google.com/search?foo",
      Value = #uri{scheme=https,
                   authority={{"bar", ""}, "www.google.com", 443},
                   path="/search",
                   query=["foo"],
                   fragment=""},
      Result = rabbitmq_aws_urilib:build(Value),
      ?assertEqual(Expect, Result)
     end},
    {"variation10", fun() ->
      Expect = "http://www.google.com/search?foo#bar",
      Value = #uri{scheme=http,
                   authority={undefined, "www.google.com", undefined},
                   path="/search",
                   query=["foo"],
                   fragment="bar"},
      Result = rabbitmq_aws_urilib:build(Value),
      ?assertEqual(Expect, Result)
     end},
    {"variation11", fun() ->
      Expect = "http://www.google.com/",
      Value = #uri{scheme=http,
                   authority={undefined, "www.google.com", undefined},
                   path=undefined,
                   query=[]},
      Result = rabbitmq_aws_urilib:build(Value),
      ?assertEqual(Expect, Result)
     end}
  ].

build_query_string_test_() ->
  [
    {"basic list", fun() ->
      ?assertEqual("foo=bar&baz=qux",
                   rabbitmq_aws_urilib:build_query_string([{"foo", "bar"},
                                                        {"baz", "qux"}]))
     end},
    {"empty list", fun() ->
      ?assertEqual("", rabbitmq_aws_urilib:build_query_string([]))
     end}
  ].

parse_test_() ->
  [
    {"variation1", fun() ->
      URI = "amqp://guest:password@rabbitmq:5672/%2F?heartbeat=5",
      Expect = #uri{scheme=amqp,
                    authority={{"guest", "password"}, "rabbitmq", 5672},
                    path="/%2F",
                    query=[{"heartbeat", "5"}],
                    fragment=undefined},
      ?assertEqual(Expect, rabbitmq_aws_urilib:parse(URI))
     end},
    {"variation2", fun() ->
      URI = "http://www.google.com/search?foo=bar#baz",
      Expect = #uri{scheme=http,
                    authority={undefined, "www.google.com", 80},
                    path="/search",
                    query=[{"foo", "bar"}],
                    fragment="#baz"},
      ?assertEqual(Expect, rabbitmq_aws_urilib:parse(URI))
     end},
    {"variation3", fun() ->
      URI = "https://www.google.com/search",
      Expect = #uri{scheme=https,
                    authority={undefined, "www.google.com", 443},
                    path="/search",
                    query=undefined,
                    fragment=undefined},
      ?assertEqual(Expect, rabbitmq_aws_urilib:parse(URI))
     end},
    {"variation4", fun() ->
      URI = "https://www.google.com/search?foo",
      Expect = #uri{scheme=https,
                    authority={undefined, "www.google.com", 443},
                    path="/search",
                    query=["foo"],
                    fragment=undefined},
      ?assertEqual(Expect, rabbitmq_aws_urilib:parse(URI))
     end},
    {"variation5", fun() ->
      URI = "https://foo@www.google.com/search?foo",
      Expect = #uri{scheme=https,
                    authority={{"foo", undefined}, "www.google.com", 443},
                    path="/search",
                    query=["foo"],
                    fragment=undefined},
      ?assertEqual(Expect, rabbitmq_aws_urilib:parse(URI))
     end},
    {"error", fun() ->
      ?assertEqual({error, no_scheme}, rabbitmq_aws_urilib:parse("hello"))
     end}
  ].


decode_test_() ->
  [
    {"percent_decode", fun() ->
        Value = "foo%2Fbar%20baz",
        Expect = "foo/bar baz",
        ?assertEqual(Expect, rabbitmq_aws_urilib:percent_decode(Value))
     end}
  ].


encode_test_() ->
  [
    {"percent_encode", fun() ->
      Value = "foo/bar baz",
      Expect = "foo%2Fbar%20baz",
      ?assertEqual(Expect, rabbitmq_aws_urilib:percent_encode(Value))
     end},
    {"percent_encode unicode", fun() ->
      Value = "foo/bar✈baz/ʘ/耀",
      Expect = "foo%2Fbar%E2%9C%88baz%2F%CA%98%2F%F0%88%80%80",
      ?assertEqual(Expect, rabbitmq_aws_urilib:percent_encode(Value))
     end}
  ].
