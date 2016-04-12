-module(httpc_aws_urilib_tests).

-include_lib("eunit/include/eunit.hrl").

-include("httpc_aws.hrl").


build_variation1_test() ->
  Expect = "amqp://guest:password@rabbitmq:5672/%2f?heartbeat=5",
  Value = #uri{scheme=amqp,
               authority={{"guest", "password"}, "rabbitmq", 5672},
               path="/%2f", query=[{"heartbeat", "5"}]},
  Result = httpc_aws_urilib:build(Value),
  ?assertEqual(Expect, Result).

build_variation2_test() ->
  Expect = "http://www.google.com/search?foo=bar#baz",
  Value = #uri{scheme=http,
               authority={{undefined, undefined}, "www.google.com", 80},
               path="/search",
               query=[{"foo", "bar"}],
               fragment="#baz"},
  Result = httpc_aws_urilib:build(Value),
  ?assertEqual(Expect, Result).

build_variation3_test() ->
  Expect = "https://www.google.com/search",
  Value = #uri{scheme=https,
               authority={{undefined, undefined}, "www.google.com", undefined},
               path="/search"},
  Result = httpc_aws_urilib:build(Value),
  ?assertEqual(Expect, Result).

build_variation4_test() ->
  Expect = "https://www.google.com/search?foo",
  Value = #uri{scheme=https,
               authority={{undefined, undefined}, "www.google.com", undefined},
               path="/search",
               query=["foo"]},
  Result = httpc_aws_urilib:build(Value),
  ?assertEqual(Expect, Result).

build_variation5_test() ->
  Expect = "https://www.google.com/search?foo",
  Value = #uri{scheme=https,
               authority={{undefined, undefined}, "www.google.com", 443},
               path="/search",
               query=["foo"]},
  Result = httpc_aws_urilib:build(Value),
  ?assertEqual(Expect, Result).

build_variation6_test() ->
  Expect = "https://bar@www.google.com/search?foo",
  Value = #uri{scheme=https,
               authority={{"bar", undefined}, "www.google.com", 443},
               path="/search",
               query=["foo"]},
  Result = httpc_aws_urilib:build(Value),
  ?assertEqual(Expect, Result).

build_variation7_test() ->
  Expect = "https://www.google.com/search?foo",
  Value = #uri{scheme=https,
               authority={undefined, "www.google.com", 443},
               path="/search",
               query=["foo"]},
  Result = httpc_aws_urilib:build(Value),
  ?assertEqual(Expect, Result).

build_variation8_test() ->
  Expect = "https://www.google.com/search?foo",
  Value = #uri{scheme=https,
               authority={{"", ""}, "www.google.com", 443},
               path="/search",
               query=["foo"]},
  Result = httpc_aws_urilib:build(Value),
  ?assertEqual(Expect, Result).

build_variation9_test() ->
  Expect = "https://bar@www.google.com/search?foo",
  Value = #uri{scheme=https,
               authority={{"bar", ""}, "www.google.com", 443},
               path="/search",
               query=["foo"],
               fragment=""},
  Result = httpc_aws_urilib:build(Value),
  ?assertEqual(Expect, Result).

build_variation10_test() ->
  Expect = "http://www.google.com/search?foo#bar",
  Value = #uri{scheme=http,
               authority={undefined, "www.google.com", undefined},
               path="/search",
               query=["foo"],
               fragment="bar"},
  Result = httpc_aws_urilib:build(Value),
  ?assertEqual(Expect, Result).

build_variation11_test() ->
  Expect = "http://www.google.com/",
  Value = #uri{scheme=http,
               authority={undefined, "www.google.com", undefined},
               path=undefined,
               query=[]},
  Result = httpc_aws_urilib:build(Value),
  ?assertEqual(Expect, Result).

parse_variation1_test() ->
    URI = "amqp://guest:password@rabbitmq:5672/%2f?heartbeat=5",
    Expect = #uri{scheme=amqp,
                  authority={{"guest", "password"}, "rabbitmq", 5672},
                  path="/%2f",
                  query=[{"heartbeat", "5"}],
                  fragment=undefined},
    ?assertEqual(Expect, httpc_aws_urilib:parse(URI)).

parse_variation2_test() ->
    URI = "http://www.google.com/search?foo=bar#baz",
    Expect = #uri{scheme=http,
                  authority={undefined, "www.google.com", 80},
                  path="/search",
                  query=[{"foo", "bar"}],
                  fragment="#baz"},
    ?assertEqual(Expect, httpc_aws_urilib:parse(URI)).

parse_variation3_test() ->
    URI = "https://www.google.com/search",
    Expect = #uri{scheme=https,
                  authority={undefined, "www.google.com", 443},
                  path="/search",
                  query=undefined,
                  fragment=undefined},
    ?assertEqual(Expect, httpc_aws_urilib:parse(URI)).

parse_variation4_test() ->
    URI = "https://www.google.com/search?foo",
    Expect = #uri{scheme=https,
                  authority={undefined, "www.google.com", 443},
                  path="/search",
                  query=["foo"],
                  fragment=undefined},
    ?assertEqual(Expect, httpc_aws_urilib:parse(URI)).

parse_variation5_test() ->
    URI = "https://foo@www.google.com/search?foo",
    Expect = #uri{scheme=https,
                  authority={{"foo", undefined}, "www.google.com", 443},
                  path="/search",
                  query=["foo"],
                  fragment=undefined},
    ?assertEqual(Expect, httpc_aws_urilib:parse(URI)).

parse_error_test() ->
    ?assertEqual({error, no_scheme}, httpc_aws_urilib:parse("hello")).

percent_decode_test() ->
    Value = "foo%2fbar%20baz",
    Expect = "foo/bar baz",
    ?assertEqual(Expect, httpc_aws_urilib:percent_decode(Value)).

plus_decode_test() ->
    Value = "foo/bar+baz",
    Expect = "foo/bar baz",
    ?assertEqual(Expect, httpc_aws_urilib:plus_decode(Value)).

percent_encode_test() ->
    Value = "foo/bar baz",
    Expect = "foo%2fbar%20baz",
    ?assertEqual(Expect, httpc_aws_urilib:percent_encode(Value)).

percent_encode_unicode_test() ->
    Value = "foo/bar✈baz",
    Expect = "foo%2fbar%c0%88baz",
    ?assertEqual(Expect, httpc_aws_urilib:percent_encode(Value)).

plus_encode_test() ->
    Value = "foo/bar baz",
    Expect = "foo%2fbar+baz",
    ?assertEqual(Expect, httpc_aws_urilib:plus_encode(Value)).
