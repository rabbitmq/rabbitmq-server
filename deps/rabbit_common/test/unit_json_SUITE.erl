%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(unit_json_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-compile(export_all).

all() ->
    [
        {group, encoding},
        {group, ip_address_encoding},
        {group, decoding}
    ].

groups() ->
    [
        {encoding, [parallel], [
            encode_primitives,
            encode_proplist_as_object,
            encode_map,
            encode_nested_structures,
            encode_function_as_string,
            try_encode_returns_ok_tuple
        ]},
        {ip_address_encoding, [parallel], [
            encode_ipv4_tuple,
            encode_ipv6_tuple,
            encode_ipv4_mapped_ipv6_tuple,
            encode_ipv4_loopback,
            encode_ipv6_loopback,
            encode_ip_tuple_inside_proplist,
            encode_ip_tuple_inside_map
        ]},
        {decoding, [parallel], [
            decode_object_to_map,
            try_decode_returns_ok_tuple,
            try_decode_returns_error_for_invalid_input
        ]}
    ].

init_per_suite(Config) -> Config.
end_per_suite(Config) -> Config.
init_per_group(_, Config) -> Config.
end_per_group(_, Config) -> Config.
init_per_testcase(_, Config) -> Config.
end_per_testcase(_, Config) -> Config.

%%
%% Encoding
%%

encode_primitives(_Config) ->
    ?assertEqual(<<"1">>, rabbit_json:encode(1)),
    ?assertEqual(<<"true">>, rabbit_json:encode(true)),
    ?assertEqual(<<"false">>, rabbit_json:encode(false)),
    ?assertEqual(<<"null">>, rabbit_json:encode(null)),
    ?assertEqual(<<"\"hello\"">>, rabbit_json:encode(<<"hello">>)).

encode_proplist_as_object(_Config) ->
    Encoded = rabbit_json:encode([{<<"a">>, 1}, {<<"b">>, 2}]),
    ?assertEqual(#{<<"a">> => 1, <<"b">> => 2}, rabbit_json:decode(Encoded)).

encode_map(_Config) ->
    Encoded = rabbit_json:encode(#{<<"k">> => <<"v">>}),
    ?assertEqual(#{<<"k">> => <<"v">>}, rabbit_json:decode(Encoded)).

encode_nested_structures(_Config) ->
    Term = [{<<"outer">>, [{<<"inner">>, [1, 2, 3]}]}],
    Encoded = rabbit_json:encode(Term),
    ?assertEqual(#{<<"outer">> => #{<<"inner">> => [1, 2, 3]}},
                 rabbit_json:decode(Encoded)).

encode_function_as_string(_Config) ->
    %% Functions are coerced to a binary representation rather than crashing.
    F = fun() -> ok end,
    Encoded = rabbit_json:encode(F),
    Decoded = rabbit_json:decode(Encoded),
    ?assert(is_binary(Decoded)).

try_encode_returns_ok_tuple(_Config) ->
    ?assertEqual({ok, <<"1">>}, rabbit_json:try_encode(1)),
    ?assertEqual({ok, <<"\"a\"">>}, rabbit_json:try_encode(<<"a">>)).

%%
%% IP address tuple encoding
%%

encode_ipv4_tuple(_Config) ->
    ?assertEqual(<<"\"192.168.1.1\"">>,
                 rabbit_json:encode({192, 168, 1, 1})).

encode_ipv6_tuple(_Config) ->
    ?assertEqual(<<"\"2001:db8::1\"">>,
                 rabbit_json:encode({16#2001, 16#db8, 0, 0, 0, 0, 0, 1})).

encode_ipv4_mapped_ipv6_tuple(_Config) ->
    %% rabbit_misc:ntoa/1 collapses IPv4-mapped IPv6 addresses back to
    %% their IPv4 form.
    ?assertEqual(<<"\"1.2.3.4\"">>,
                 rabbit_json:encode({0, 0, 0, 0, 0, 16#ffff, 16#0102, 16#0304})).

encode_ipv4_loopback(_Config) ->
    ?assertEqual(<<"\"127.0.0.1\"">>,
                 rabbit_json:encode({127, 0, 0, 1})).

encode_ipv6_loopback(_Config) ->
    ?assertEqual(<<"\"::1\"">>,
                 rabbit_json:encode({0, 0, 0, 0, 0, 0, 0, 1})).

encode_ip_tuple_inside_proplist(_Config) ->
    %% This mirrors the real-world shape of a connection_created event
    %% with an unformatted peer_addr.
    Encoded = rabbit_json:encode([{<<"peer_addr">>, {10, 0, 0, 1}}]),
    ?assertEqual(#{<<"peer_addr">> => <<"10.0.0.1">>},
                 rabbit_json:decode(Encoded)).

encode_ip_tuple_inside_map(_Config) ->
    Encoded = rabbit_json:encode(#{<<"peer_addr">> => {10, 0, 0, 1}}),
    ?assertEqual(#{<<"peer_addr">> => <<"10.0.0.1">>},
                 rabbit_json:decode(Encoded)).

%%
%% Decoding
%%

decode_object_to_map(_Config) ->
    ?assertEqual(#{<<"a">> => 1, <<"b">> => [1, 2]},
                 rabbit_json:decode(<<"{\"a\":1,\"b\":[1,2]}">>)).

try_decode_returns_ok_tuple(_Config) ->
    ?assertEqual({ok, #{<<"a">> => 1}},
                 rabbit_json:try_decode(<<"{\"a\":1}">>)).

try_decode_returns_error_for_invalid_input(_Config) ->
    ?assertMatch({error, _}, rabbit_json:try_decode(<<"{not json">>)).
