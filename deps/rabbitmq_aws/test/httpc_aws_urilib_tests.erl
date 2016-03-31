-module(httpc_aws_urilib_tests).

-include_lib("eunit/include/eunit.hrl").

build_variation1_test() ->
    Params = {amqp, {{"guest", "password"}, "rabbitmq", 5672}, "/%2f", [{"heartbeat", "5"}], undefined},
    Expect = "amqp://guest:password@rabbitmq:5672/%2f?heartbeat=5",
    ?assertEqual(Expect, httpc_aws_urilib:build(Params)).

build_variation2_test() ->
    Params = {http, {undefined, "www.google.com", 80}, "/search", [{"foo", "bar"}], "#baz"},
    Expect = "http://www.google.com/search?foo=bar#baz",
    ?assertEqual(Expect, httpc_aws_urilib:build(Params)).

build_variation3_test() ->
    Params = {https, {undefined, "www.google.com", 443}, "/search", undefined, undefined},
    Expect = "https://www.google.com/search",
    ?assertEqual(Expect, httpc_aws_urilib:build(Params)).

build_variation4_test() ->
    Params = {https, {undefined, "www.google.com", 443}, "/search", ["foo"], undefined},
    Expect = "https://www.google.com/search?foo",
    ?assertEqual(Expect, httpc_aws_urilib:build(Params)).

build_variation5_test() ->
    Params = {https, {undefined, "www.google.com", undefined}, "/search", ["foo"], undefined},
    Expect = "https://www.google.com/search?foo",
    ?assertEqual(Expect, httpc_aws_urilib:build(Params)).

build_variation6_test() ->
    Params = {http, {undefined, "www.google.com", undefined}, "/search", ["foo"], undefined},
    Expect = "http://www.google.com/search?foo",
    ?assertEqual(Expect, httpc_aws_urilib:build(Params)).

build_variation7_test() ->
    Params = {undefined, {undefined, "www.google.com", undefined}, "/search", ["foo"], undefined},
    Expect = "http://www.google.com/search?foo",
    ?assertEqual(Expect, httpc_aws_urilib:build(Params)).

build_variation8_test() ->
    Params = {undefined, {undefined, "www.google.com", undefined}, undefined, ["foo"], undefined},
    Expect = "http://www.google.com/?foo",
    ?assertEqual(Expect, httpc_aws_urilib:build(Params)).

build_variation9_test() ->
    Params = {undefined, {undefined, "www.google.com", undefined}, undefined, [], ""},
    Expect = "http://www.google.com/",
    ?assertEqual(Expect, httpc_aws_urilib:build(Params)).

build_variation10_test() ->
    Params = {undefined, {undefined, "www.google.com", undefined}, undefined, [], "foo"},
    Expect = "http://www.google.com/#foo",
    ?assertEqual(Expect, httpc_aws_urilib:build(Params)).

build_url_variation1_test() ->
    Params = {amqp, "guest", "password", "rabbitmq", 5672, "/%2f", [{"heartbeat", "5"}], undefined},
    Expect = "amqp://guest:password@rabbitmq:5672/%2f?heartbeat=5",
    ?assertEqual(Expect, httpc_aws_urilib:build(Params)).

build_url_variation2_test() ->
    Params = {http, undefined, "www.google.com", 80, "/search", [{"foo", "bar"}], "#baz"},
    Expect = "http://www.google.com/search?foo=bar#baz",
    ?assertEqual(Expect, httpc_aws_urilib:build(Params)).

build_url_variation3_test() ->
    Params = {https, undefined, "www.google.com", 443, "/search", undefined, undefined},
    Expect = "https://www.google.com/search",
    ?assertEqual(Expect, httpc_aws_urilib:build(Params)).

build_url_variation4_test() ->
    Params = {https, undefined, "www.google.com", 443, "/search", ["foo"], undefined},
    Expect = "https://www.google.com/search?foo",
    ?assertEqual(Expect, httpc_aws_urilib:build(Params)).

build_url_variation5_test() ->
    Params = {https, "", "", "www.google.com", 443, "/search", ["foo"], undefined},
    Expect = "https://www.google.com/search?foo",
    ?assertEqual(Expect, httpc_aws_urilib:build(Params)).

build_url_variation6_test() ->
    Params = {https, "bar", "", "www.google.com", 443, "/search", ["foo"], undefined},
    Expect = "https://bar@www.google.com/search?foo",
    ?assertEqual(Expect, httpc_aws_urilib:build(Params)).

build_url_variation7_test() ->
    Params = {https, "bar", undefined, "www.google.com", 443, "/search", ["foo"], undefined},
    Expect = "https://bar@www.google.com/search?foo",
    ?assertEqual(Expect, httpc_aws_urilib:build(Params)).

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

parse_uri_test() ->
    URI = "amqp://guest:password@rabbitmq:5672/%2f?heartbeat=5",
    Expect = {amqp, {{"guest", "password"}, "rabbitmq", 5672}, "/%2f",
              [{"heartbeat", "5"}], undefined},
    ?assertEqual(Expect, httpc_aws_urilib:parse(URI, uri)).

parse_url_variation1_test() ->
    URI = "amqp://guest:password@rabbitmq:5672/%2f?heartbeat=5&foo=bar&baz+corgie=qux+grault",
    Expect = {amqp, "guest", "password", "rabbitmq", 5672, "/%2f",
              [{"heartbeat", "5"}, {"foo", "bar"}, {"baz corgie", "qux grault"}],
              undefined},
    ?assertEqual(Expect, httpc_aws_urilib:parse(URI, url)).

parse_url_variation2_test() ->
    URI = "amqp://guest@rabbitmq:5672/%2f?heartbeat=5&foo=bar&baz+corgie=qux+grault#foo",
    Expect = {amqp, "guest", undefined, "rabbitmq", 5672, "/%2f",
              [{"heartbeat", "5"}, {"foo", "bar"}, {"baz corgie", "qux grault"}],
              "#foo"},
    ?assertEqual(Expect, httpc_aws_urilib:parse(URI, url)).

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
