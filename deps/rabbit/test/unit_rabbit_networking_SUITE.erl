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
          trusted_proxy_source_empty_list_denies_everything,
          trusted_proxy_source_exact_ipv4_match,
          trusted_proxy_source_ipv4_cidr_match,
          trusted_proxy_source_ipv4_cidr_no_match,
          trusted_proxy_source_ipv4_slash_32_is_exact,
          trusted_proxy_source_ipv4_slash_0_matches_any,
          trusted_proxy_source_exact_ipv6_match,
          trusted_proxy_source_ipv6_cidr_match,
          trusted_proxy_source_mismatched_family_does_not_match,
          trusted_proxy_source_malformed_entry_does_not_match_or_crash,
          trusted_proxy_source_malformed_term_entry_does_not_match_or_crash
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

trusted_proxy_source_empty_list_denies_everything(_Config) ->
    with_trusted_proxies(
      [],
      fun() ->
              ?assertNot(rabbit_networking:is_trusted_proxy_source({127, 0, 0, 1}))
      end).

trusted_proxy_source_exact_ipv4_match(_Config) ->
    with_trusted_proxies(
      ["10.0.0.5"],
      fun() ->
              ?assert(rabbit_networking:is_trusted_proxy_source({10, 0, 0, 5})),
              ?assertNot(rabbit_networking:is_trusted_proxy_source({10, 0, 0, 6}))
      end).

trusted_proxy_source_ipv4_cidr_match(_Config) ->
    with_trusted_proxies(
      ["10.0.0.0/8"],
      fun() ->
              ?assert(rabbit_networking:is_trusted_proxy_source({10, 1, 2, 3})),
              ?assert(rabbit_networking:is_trusted_proxy_source({10, 255, 255, 255}))
      end).

trusted_proxy_source_ipv4_cidr_no_match(_Config) ->
    with_trusted_proxies(
      ["10.0.0.0/8"],
      fun() ->
              ?assertNot(rabbit_networking:is_trusted_proxy_source({11, 0, 0, 1}))
      end).

trusted_proxy_source_ipv4_slash_32_is_exact(_Config) ->
    with_trusted_proxies(
      ["192.168.1.1/32"],
      fun() ->
              ?assert(rabbit_networking:is_trusted_proxy_source({192, 168, 1, 1})),
              ?assertNot(rabbit_networking:is_trusted_proxy_source({192, 168, 1, 2}))
      end).

trusted_proxy_source_ipv4_slash_0_matches_any(_Config) ->
    with_trusted_proxies(
      ["0.0.0.0/0"],
      fun() ->
              ?assert(rabbit_networking:is_trusted_proxy_source({203, 0, 113, 42}))
      end).

trusted_proxy_source_exact_ipv6_match(_Config) ->
    with_trusted_proxies(
      ["::1"],
      fun() ->
              ?assert(rabbit_networking:is_trusted_proxy_source({0, 0, 0, 0, 0, 0, 0, 1})),
              ?assertNot(rabbit_networking:is_trusted_proxy_source({0, 0, 0, 0, 0, 0, 0, 2}))
      end).

trusted_proxy_source_ipv6_cidr_match(_Config) ->
    with_trusted_proxies(
      ["fd00::/8"],
      fun() ->
              ?assert(rabbit_networking:is_trusted_proxy_source(
                        {16#fd00, 0, 0, 0, 0, 0, 0, 1})),
              ?assertNot(rabbit_networking:is_trusted_proxy_source(
                           {16#fe00, 0, 0, 0, 0, 0, 0, 1}))
      end).

trusted_proxy_source_mismatched_family_does_not_match(_Config) ->
    with_trusted_proxies(
      ["10.0.0.0/8"],
      fun() ->
              ?assertNot(rabbit_networking:is_trusted_proxy_source(
                           {0, 0, 0, 0, 0, 0, 0, 1}))
      end).

trusted_proxy_source_malformed_entry_does_not_match_or_crash(_Config) ->
    with_trusted_proxies(
      ["not-an-ip", "10.0.0.0/not-a-number", "10.0.0.0/8"],
      fun() ->
              ?assert(rabbit_networking:is_trusted_proxy_source({10, 1, 2, 3}))
      end).

%% Only rabbitmq.conf-sourced entries are guaranteed to be strings;
%% advanced.config/application:set_env can set arbitrary Erlang terms.
%% rabbit_data_coercion:to_list/1 has no catch-all clause, so an
%% out-of-range or wrong-arity tuple must not crash the whole check
%% (and, transitively, every connection handshake on the listener).
trusted_proxy_source_malformed_term_entry_does_not_match_or_crash(_Config) ->
    with_trusted_proxies(
      [{256, 0, 0, 1}, {1, 2, 3}, undefined, self(), "10.0.0.0/8"],
      fun() ->
              ?assert(rabbit_networking:is_trusted_proxy_source({10, 1, 2, 3})),
              ?assertNot(rabbit_networking:is_trusted_proxy_source({11, 1, 2, 3}))
      end).

with_trusted_proxies(TrustedProxies, Fun) ->
    Previous = application:get_env(rabbit, proxy_protocol_trusted_proxies),
    ok = application:set_env(rabbit, proxy_protocol_trusted_proxies, TrustedProxies),
    try
        Fun()
    after
        case Previous of
            {ok, Value} -> application:set_env(rabbit, proxy_protocol_trusted_proxies, Value);
            undefined -> application:unset_env(rabbit, proxy_protocol_trusted_proxies)
        end
    end.
