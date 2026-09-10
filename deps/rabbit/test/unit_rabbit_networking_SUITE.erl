%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(unit_rabbit_networking_SUITE).

-include_lib("eunit/include/eunit.hrl").

-compile(export_all).

all() ->
    [
      {group, parallel_tests}
    ].

groups() ->
    [
      {parallel_tests, [], [
          unset_ip_resolves_to_every_listener_address,
          unset_ip_yields_one_listener_per_address,
          expanded_listeners_keep_the_other_options,
          expanded_listeners_have_distinct_ranch_refs,
          ranch_ref_names_the_first_address,
          ip_tuple_is_used_as_is,
          ip_string_is_parsed,
          ip_binary_is_parsed
        ]}
    ].

unset_ip_resolves_to_every_listener_address(_Config) ->
    Expected = [IPAddress || {IPAddress, _Port, _Family}
                                 <- rabbit_networking:tcp_listener_addresses(15672)],
    ?assertEqual(Expected,
                 rabbit_networking:listener_ip_addresses([{port, 15672}])).

unset_ip_yields_one_listener_per_address(_Config) ->
    Listener = [{port, 15672}],
    ?assertEqual(rabbit_networking:listener_ip_addresses(Listener),
                 [proplists:get_value(ip, L)
                  || L <- rabbit_networking:listener_per_ip_address(Listener)]).

expanded_listeners_keep_the_other_options(_Config) ->
    [L | _] = rabbit_networking:listener_per_ip_address(
                [{port, 15672}, {max_connections, 100}]),
    ?assertEqual(15672, proplists:get_value(port, L)),
    ?assertEqual(100, proplists:get_value(max_connections, L)).

expanded_listeners_have_distinct_ranch_refs(_Config) ->
    Refs = [rabbit_networking:ranch_ref(L)
            || L <- rabbit_networking:listener_per_ip_address([{port, 15672}])],
    ?assertEqual(length(Refs), length(lists:usort(Refs))).

ranch_ref_names_the_first_address(_Config) ->
    Listener = [{port, 15672}],
    [IPAddress | _] = rabbit_networking:listener_ip_addresses(Listener),
    ?assertEqual({acceptor, IPAddress, 15672},
                 rabbit_networking:ranch_ref(Listener)).

ip_tuple_is_used_as_is(_Config) ->
    ?assertEqual([{127, 0, 0, 1}],
                 rabbit_networking:listener_ip_addresses(
                   [{port, 15672}, {ip, {127, 0, 0, 1}}])).

ip_string_is_parsed(_Config) ->
    ?assertEqual([{0, 0, 0, 0, 0, 0, 0, 1}],
                 rabbit_networking:listener_ip_addresses(
                   [{port, 15672}, {ip, "::1"}])).

ip_binary_is_parsed(_Config) ->
    ?assertEqual([{127, 0, 0, 1}],
                 rabbit_networking:listener_ip_addresses(
                   [{port, 15672}, {ip, <<"127.0.0.1">>}])).
