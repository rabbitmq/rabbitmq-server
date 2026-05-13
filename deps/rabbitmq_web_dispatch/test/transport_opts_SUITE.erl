%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(transport_opts_SUITE).

-compile([export_all, nowarn_export_all]).

-include_lib("proper/include/proper.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(BUILD(Opts),
        rabbit_web_dispatch_sup:build_ranch_transport_opts(Opts)).
-define(PROP_ITERATIONS, 500).

all() ->
    [{group, unit_tests},
     {group, property_tests}].

groups() ->
    [{unit_tests, [parallel],
      [no_ranch_opts_returns_input_list,
       empty_proplist_returns_empty_list,
       max_connections_lifted_to_map,
       num_acceptors_lifted_to_map,
       num_conns_sups_lifted_to_map,
       all_ranch_opts_together,
       infinity_value_is_preserved,
       socket_opts_preserved_alongside_ranch_opt,
       bare_atoms_stay_in_socket_opts,
       ranch_opt_value_passed_through_verbatim]},
     {property_tests, [],
      [prop_legacy_form_is_passthrough,
       prop_map_form_when_any_ranch_opt,
       prop_no_ranch_opt_in_socket_opts,
       prop_non_ranch_entries_preserved_in_socket_opts,
       prop_ranch_opt_values_preserved]}].

init_per_suite(Config) -> Config.
end_per_suite(_Config) -> ok.

init_per_group(_, Config) -> Config.
end_per_group(_, Config) -> Config.

init_per_testcase(_, Config) -> Config.
end_per_testcase(_, Config) -> Config.

%%--------------------------------------------------------------------
%% Unit cases
%%--------------------------------------------------------------------

no_ranch_opts_returns_input_list(_) ->
    Input = [{port, 15672}, {ip, {127, 0, 0, 1}}, {backlog, 128}],
    ?assertEqual(Input, ?BUILD(Input)).

empty_proplist_returns_empty_list(_) ->
    ?assertEqual([], ?BUILD([])).

max_connections_lifted_to_map(_) ->
    Result = ?BUILD([{port, 15672}, {max_connections, 1024}]),
    ?assertMatch(#{max_connections := 1024,
                   socket_opts     := [{port, 15672}]},
                 Result),
    ?assertNot(maps:is_key(num_acceptors, Result)).

num_acceptors_lifted_to_map(_) ->
    Result = ?BUILD([{port, 15672}, {num_acceptors, 16}]),
    ?assertMatch(#{num_acceptors := 16,
                   socket_opts   := [{port, 15672}]},
                 Result).

num_conns_sups_lifted_to_map(_) ->
    Result = ?BUILD([{port, 15672}, {num_conns_sups, 4}]),
    ?assertMatch(#{num_conns_sups := 4,
                   socket_opts    := [{port, 15672}]},
                 Result).

all_ranch_opts_together(_) ->
    Result = ?BUILD([{port, 15672},
                     {ip, {127, 0, 0, 1}},
                     {max_connections, 2048},
                     {num_acceptors, 8},
                     {num_conns_sups, 2}]),
    ?assertMatch(#{max_connections := 2048,
                   num_acceptors   := 8,
                   num_conns_sups  := 2}, Result),
    SocketOpts = maps:get(socket_opts, Result),
    ?assertEqual(lists:sort([{port, 15672}, {ip, {127, 0, 0, 1}}]),
                 lists:sort(SocketOpts)).

infinity_value_is_preserved(_) ->
    ?assertMatch(#{max_connections := infinity},
                 ?BUILD([{max_connections, infinity}])).

socket_opts_preserved_alongside_ranch_opt(_) ->
    Socket = [{port, 15672}, {ip, {127, 0, 0, 1}}, {backlog, 256}],
    Result = ?BUILD(Socket ++ [{max_connections, 100}]),
    ?assertEqual(lists:sort(Socket),
                 lists:sort(maps:get(socket_opts, Result))).

bare_atoms_stay_in_socket_opts(_) ->
    Result = ?BUILD([binary, {port, 15672}, {max_connections, 10}]),
    SocketOpts = maps:get(socket_opts, Result),
    ?assert(lists:member(binary, SocketOpts)),
    ?assert(lists:member({port, 15672}, SocketOpts)).

ranch_opt_value_passed_through_verbatim(_) ->
    Sentinel = {tagged, 42},
    Result = ?BUILD([{max_connections, Sentinel}]),
    ?assertMatch(#{max_connections := Sentinel}, Result).

%%--------------------------------------------------------------------
%% Property cases
%%--------------------------------------------------------------------

prop_legacy_form_is_passthrough(_) ->
    rabbit_ct_proper_helpers:run_proper(
      fun () ->
              ?FORALL(Opts, socket_only_proplist(),
                      ?BUILD(Opts) =:= Opts)
      end, [], ?PROP_ITERATIONS).

prop_map_form_when_any_ranch_opt(_) ->
    rabbit_ct_proper_helpers:run_proper(
      fun () ->
              ?FORALL(Opts, proplist_with_ranch_opt(),
                      is_map(?BUILD(Opts)))
      end, [], ?PROP_ITERATIONS).

prop_no_ranch_opt_in_socket_opts(_) ->
    rabbit_ct_proper_helpers:run_proper(
      fun () ->
              ?FORALL(Opts, proplist_with_ranch_opt(),
                      begin
                          #{socket_opts := SocketOpts} = ?BUILD(Opts),
                          lists:all(fun(E) -> not is_ranch_entry(E) end,
                                    SocketOpts)
                      end)
      end, [], ?PROP_ITERATIONS).

prop_non_ranch_entries_preserved_in_socket_opts(_) ->
    rabbit_ct_proper_helpers:run_proper(
      fun () ->
              ?FORALL(Opts, proplist_with_ranch_opt(),
                      begin
                          #{socket_opts := SocketOpts} = ?BUILD(Opts),
                          Expected = [E || E <- Opts, not is_ranch_entry(E)],
                          lists:sort(SocketOpts) =:= lists:sort(Expected)
                      end)
      end, [], ?PROP_ITERATIONS).

prop_ranch_opt_values_preserved(_) ->
    rabbit_ct_proper_helpers:run_proper(
      fun () ->
              ?FORALL(Opts, proplist_with_ranch_opt(),
                      begin
                          Map = ?BUILD(Opts),
                          lists:all(
                            fun({K, V}) ->
                                    maps:get(K, Map) =:= V
                            end,
                            [{K, proplists:get_value(K, Opts)}
                             || K <- ranch_keys(),
                                proplists:is_defined(K, Opts)])
                      end)
      end, [], ?PROP_ITERATIONS).

%%--------------------------------------------------------------------
%% Generators and helpers
%%--------------------------------------------------------------------

ranch_keys() ->
    [max_connections, num_acceptors, num_conns_sups].

is_ranch_entry({K, _}) -> lists:member(K, ranch_keys());
is_ranch_entry(_)       -> false.

socket_entry() ->
    oneof([{port, pos_integer()},
           {ip, ip_tuple()},
           {backlog, pos_integer()},
           {nodelay, boolean()},
           binary,
           {keepalive, boolean()}]).

ip_tuple() ->
    {integer(0, 255), integer(0, 255), integer(0, 255), integer(0, 255)}.

socket_only_proplist() ->
    list(socket_entry()).

ranch_entry() ->
    oneof([{max_connections, oneof([infinity, non_neg_integer()])},
           {num_acceptors, pos_integer()},
           {num_conns_sups, pos_integer()}]).

%% Always at least one Ranch entry.
proplist_with_ranch_opt() ->
    ?LET({RanchHead, RanchRest, Sockets},
         {ranch_entry(), list(ranch_entry()), socket_only_proplist()},
         [RanchHead | RanchRest] ++ Sockets).
