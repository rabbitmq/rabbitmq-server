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
          unset_ip_resolves_to_wildcard_addresses,
          unset_ip_yields_one_listener_per_address,
          explicit_ip_yields_a_single_listener,
          expanded_listeners_keep_the_other_options,
          expanded_listeners_have_distinct_ranch_refs,
          ranch_ref_names_the_first_address,
          ip_tuple_is_used_as_is,
          ip_string_is_parsed,
          ip_binary_is_parsed,
          trusted_proxies_unconfigured_omits_option,
          trusted_proxies_addresses_and_cidrs,
          trusted_proxies_invalid_entries_are_ignored,
          trusted_proxies_invalid_entries_do_not_allow_any_peer,
          trusted_proxies_mapped_ipv6_entries_are_ignored
        ]}
    ].

unset_ip_resolves_to_wildcard_addresses(_Config) ->
    Addresses = rabbit_networking:listener_ip_addresses([{port, 15672}]),
    ?assertNotEqual([], Addresses),
    ?assertEqual([], Addresses -- [{0, 0, 0, 0}, {0, 0, 0, 0, 0, 0, 0, 0}]),
    ?assertEqual(length(Addresses), length(lists:usort(Addresses))).

unset_ip_yields_one_listener_per_address(_Config) ->
    Listener = [{port, 15672}],
    ?assertEqual(rabbit_networking:listener_ip_addresses(Listener),
                 [proplists:get_value(ip, L)
                  || L <- rabbit_networking:listener_per_ip_address(Listener)]).

explicit_ip_yields_a_single_listener(_Config) ->
    [Expanded] = rabbit_networking:listener_per_ip_address(
                   [{port, 15672}, {ip, "127.0.0.1"}]),
    ?assertEqual([{ip, {127, 0, 0, 1}}], proplists:lookup_all(ip, Expanded)),
    ?assertEqual(15672, proplists:get_value(port, Expanded)).

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

trusted_proxies_unconfigured_omits_option(_Config) ->
    with_trusted_proxies([], fun() ->
        ?assertEqual(#{}, rabbit_networking:proxy_protocol_options())
    end),
    Previous = application:get_env(rabbit, proxy_protocol_trusted_proxies),
    try
        application:unset_env(rabbit, proxy_protocol_trusted_proxies),
        ?assertEqual(#{}, rabbit_networking:proxy_protocol_options())
    after
        restore_trusted_proxies(Previous)
    end.

trusted_proxies_addresses_and_cidrs(_Config) ->
    with_trusted_proxies(
      ["10.0.0.5", "10.1.2.3/8", "192.168.1.1/32", "0.0.0.0/0",
       "::1", "fd00::/8", "::1/128", "::/0", <<"127.0.0.1">>],
      fun() ->
          ?assertEqual(#{trusted_proxies =>
              [{{10, 0, 0, 5}, 32}, {{10, 1, 2, 3}, 8},
               {{192, 168, 1, 1}, 32}, {{0, 0, 0, 0}, 0},
               {{0, 0, 0, 0, 0, 0, 0, 1}, 128},
               {{16#fd00, 0, 0, 0, 0, 0, 0, 0}, 8},
               {{0, 0, 0, 0, 0, 0, 0, 1}, 128},
               {{0, 0, 0, 0, 0, 0, 0, 0}, 0},
               {{127, 0, 0, 1}, 32}]},
              rabbit_networking:proxy_protocol_options())
      end).

trusted_proxies_invalid_entries_are_ignored(_Config) ->
    with_trusted_proxies(
      invalid_trusted_proxies() ++ ["10.0.0.0/8"],
      fun() ->
          ?assertEqual(#{trusted_proxies => [{{10, 0, 0, 0}, 8}]},
                       rabbit_networking:proxy_protocol_options())
      end).

trusted_proxies_invalid_entries_do_not_allow_any_peer(_Config) ->
    with_trusted_proxies(invalid_trusted_proxies(), fun() ->
        ?assertEqual(#{trusted_proxies => []}, rabbit_networking:proxy_protocol_options())
    end).

trusted_proxies_mapped_ipv6_entries_are_ignored(_Config) ->
    with_trusted_proxies(["::ffff:127.0.0.1", "::ffff:127.0.0.0/104"], fun() ->
        ?assertEqual(#{trusted_proxies => []}, rabbit_networking:proxy_protocol_options())
    end).

invalid_trusted_proxies() ->
    ["not-an-ip", "10.0.0.0/not-a-number", "10.0.0.0/8/8",
     "10.0.0.0/-1", "10.0.0.0/33", "::/-1", "::/129", "::/8x",
     {127, 0, 0, 2}, {256, 0, 0, 1}, {1, 2, 3}, undefined, self()].

with_trusted_proxies(TrustedProxies, Fun) ->
    Previous = application:get_env(rabbit, proxy_protocol_trusted_proxies),
    ok = application:set_env(rabbit, proxy_protocol_trusted_proxies, TrustedProxies),
    try
        Fun()
    after
        restore_trusted_proxies(Previous)
    end.

restore_trusted_proxies({ok, Value}) ->
    application:set_env(rabbit, proxy_protocol_trusted_proxies, Value);
restore_trusted_proxies(undefined) ->
    application:unset_env(rabbit, proxy_protocol_trusted_proxies).
