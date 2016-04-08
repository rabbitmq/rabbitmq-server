-module(httpc_aws_urilib_tests).

-include_lib("eunit/include/eunit.hrl").

build_variation1_test() ->
    Expect = "amqp://guest:password@rabbitmq:5672/%2f?heartbeat=5",
    Result = httpc_aws_urilib:build("amqp", "guest", "password", "rabbitmq", 5672, "/%2f", [{"heartbeat", "5"}], undefined),
    ?assertEqual(Expect, Result).

build_variation2_test() ->
  Expect = "http://www.google.com/search?foo=bar#baz",
  Result = httpc_aws_urilib:build("http", undefined, undefined, "www.google.com", 80, "/search", [{"foo", "bar"}], "#baz"),
  ?assertEqual(Expect, Result).

build_variation3_test() ->
    Expect = "https://www.google.com/search",
    Result = httpc_aws_urilib:build("https", undefined, undefined, "www.google.com", 443, "/search", undefined, undefined),
    ?assertEqual(Expect, Result).

build_variation4_test() ->
    Expect = "https://www.google.com/search?foo",
    Result = httpc_aws_urilib:build("https", undefined, undefined, "www.google.com", 443, "/search", ["foo"], undefined),
    ?assertEqual(Expect, Result).

build_variation5_test() ->
    Expect = "https://www.google.com/search?foo",
    Result = httpc_aws_urilib:build("https", "", "", "www.google.com", 443, "/search", ["foo"], undefined),
    ?assertEqual(Expect, Result).

build_variation6_test() ->
    Expect = "https://bar@www.google.com/search?foo",
    Result = httpc_aws_urilib:build("https", "bar", "", "www.google.com", 443, "/search", ["foo"], undefined),
    ?assertEqual(Expect, Result).

build_variation7_test() ->
    Expect = "https://bar@www.google.com/search?foo",
    Result = httpc_aws_urilib:build("https", "bar", undefined, "www.google.com", 443, "/search", ["foo"], undefined),
    ?assertEqual(Expect, Result).

parse_variation1_test() ->
    URI = "amqp://guest:password@rabbitmq:5672/%2f?heartbeat=5",
    Expect = {amqp, {{"guest", "password"}, "rabbitmq", 5672}, "/%2f", [{"heartbeat", "5"}], undefined},
    ?assertEqual(Expect, httpc_aws_urilib:parse(URI)).

parse_variation2_test() ->
    URI = "http://www.google.com/search?foo=bar#baz",
    Expect = {http, {undefined, "www.google.com", 80}, "/search", [{"foo", "bar"}], "#baz"},
    ?assertEqual(Expect, httpc_aws_urilib:parse(URI)).

parse_variation3_test() ->
    URI = "https://www.google.com/search",
    Expect = {https, {undefined, "www.google.com", 443}, "/search", undefined, undefined},
    ?assertEqual(Expect, httpc_aws_urilib:parse(URI)).

parse_variation4_test() ->
    URI = "https://www.google.com/search?foo",
    Expect = {https, {undefined, "www.google.com", 443}, "/search", ["foo"], undefined},
    ?assertEqual(Expect, httpc_aws_urilib:parse(URI)).

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
