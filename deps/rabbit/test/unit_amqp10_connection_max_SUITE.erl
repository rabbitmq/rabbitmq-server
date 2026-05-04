%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(unit_amqp10_connection_max_SUITE).

-compile([export_all, nowarn_export_all]).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("amqp10_common/include/amqp10_framing.hrl").

-define(REF, {acceptor, {0,0,0,0}, 5672}).

all() ->
    [{group, predicate}].

groups() ->
    [{predicate, [],
      [
       infinity_always_admits,
       undefined_ranch_ref_always_admits,
       limit_zero_refuses_first_connection,
       limit_admits_when_active_below_or_at_limit,
       limit_refuses_when_active_exceeds,
       refusal_uses_resource_limit_exceeded_condition
      ]}].

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    ok.

init_per_testcase(_Testcase, Config) ->
    ok = meck:new(ranch, [passthrough]),
    application:set_env(rabbit, connection_max, infinity),
    Config.

end_per_testcase(_Testcase, _Config) ->
    application:unset_env(rabbit, connection_max),
    ok = meck:unload(ranch),
    ok.

%% -------------------------------------------------------------------
%% Test cases
%% -------------------------------------------------------------------

infinity_always_admits(_Config) ->
    application:set_env(rabbit, connection_max, infinity),
    mock_active_connections(10_000_000),
    ?assertEqual(ok, check(?REF)),
    ok.

%% AMQP-over-WebSockets has no Ranch listener context, so the
%% predicate must short-circuit.
undefined_ranch_ref_always_admits(_Config) ->
    application:set_env(rabbit, connection_max, 0),
    ?assertEqual(ok, check(undefined)),
    ok.

%% Ranch counts the current connection in `active_connections`, so a
%% limit of 0 leads to all new connections being immediately refused.
limit_zero_refuses_first_connection(_Config) ->
    application:set_env(rabbit, connection_max, 0),
    mock_active_connections(1),
    ?assertMatch({refused, _}, exit_to_error_tuple(?REF)),
    ok.

limit_admits_when_active_below_or_at_limit(_Config) ->
    application:set_env(rabbit, connection_max, 5),
    [begin
         mock_active_connections(N),
         ?assertEqual(ok, check(?REF))
     end || N <- lists:seq(1, 5)],
    ok.

limit_refuses_when_active_exceeds(_Config) ->
    application:set_env(rabbit, connection_max, 5),
    mock_active_connections(6),
    ?assertMatch({refused, _}, exit_to_error_tuple(?REF)),
    ok.

refusal_uses_resource_limit_exceeded_condition(_Config) ->
    application:set_env(rabbit, connection_max, 0),
    mock_active_connections(1),
    {refused, Reason} = exit_to_error_tuple(?REF),
    ?assertMatch(#'v1_0.error'{
                    condition = ?V_1_0_AMQP_ERROR_RESOURCE_LIMIT_EXCEEDED}, Reason),
    ok.

%% -------------------------------------------------------------------
%% Helpers
%% -------------------------------------------------------------------

check(Ref) ->
    rabbit_amqp_reader:check_node_connection_limit(Ref).

exit_to_error_tuple(Ref) ->
    try check(Ref) of
        ok -> ok
    catch
        exit:#'v1_0.error'{} = E -> {refused, E}
    end.

mock_active_connections(N) ->
    meck:expect(ranch, info, fun (_Ref) -> #{active_connections => N} end).
