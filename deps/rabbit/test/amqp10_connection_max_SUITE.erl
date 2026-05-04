%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(amqp10_connection_max_SUITE).

-compile([export_all, nowarn_export_all]).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").
-include_lib("amqp10_common/include/amqp10_framing.hrl").

-define(TIMEOUT, 30_000).

all() ->
    [{group, single_node}].

groups() ->
    [{single_node, [],
      [
       node_connection_limit_amqp10,
       node_limit_infinity_allows_amqp10,
       node_limit_room_after_close,
       node_connection_limit_shared_with_amqp091,
       amqp10_connection_is_tracked
      ]}].

suite() ->
    [{timetrap, {minutes, 3}}].

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(amqp10_client),
    rabbit_ct_helpers:log_environment(),
    rabbit_ct_helpers:run_setup_steps(Config).

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config).

init_per_group(Group, Config0) ->
    Config1 = rabbit_ct_helpers:set_config(Config0,
                                           [{rmq_nodename_suffix, Group},
                                            {rmq_nodes_count, 1}]),
    rabbit_ct_helpers:run_steps(Config1,
                                rabbit_ct_broker_helpers:setup_steps() ++
                                    rabbit_ct_client_helpers:setup_steps()).

end_per_group(_Group, Config) ->
    rabbit_ct_helpers:run_steps(Config,
                                rabbit_ct_client_helpers:teardown_steps() ++
                                    rabbit_ct_broker_helpers:teardown_steps()).

init_per_testcase(Testcase, Config) ->
    set_node_limit(Config, infinity),
    rabbit_ct_helpers:testcase_started(Config, Testcase).

end_per_testcase(Testcase, Config) ->
    set_node_limit(Config, infinity),
    rabbit_ct_helpers:testcase_finished(Config, Testcase).

%% -------------------------------------------------------------------
%% Test cases
%% -------------------------------------------------------------------

node_connection_limit_amqp10(Config) ->
    set_node_limit(Config, 0),
    {refused, _} = open_amqp10(Config),

    set_node_limit(Config, 5),
    Conns = [open_amqp10_ok(Config) || _ <- lists:seq(1, 5)],
    {refused, _} = open_amqp10(Config),
    [amqp_utils:close_connection_sync(C) || C <- Conns],
    ok.

node_limit_infinity_allows_amqp10(Config) ->
    set_node_limit(Config, infinity),
    Conns = [open_amqp10_ok(Config) || _ <- lists:seq(1, 10)],
    [amqp_utils:close_connection_sync(C) || C <- Conns],
    ok.

node_limit_room_after_close(Config) ->
    set_node_limit(Config, 2),
    C1 = open_amqp10_ok(Config),
    C2 = open_amqp10_ok(Config),
    {refused, _} = open_amqp10(Config),
    amqp_utils:close_connection_sync(C1),
    {ok, C3} = retry_open_amqp10(Config, 30),
    [amqp_utils:close_connection_sync(C) || C <- [C2, C3]],
    ok.

%% Historically only AMQP 0-9-1 honored `connection_max`,
%% so this example uses both protocols.
node_connection_limit_shared_with_amqp091(Config) ->
    set_node_limit(Config, 2),
    C091 = rabbit_ct_client_helpers:open_unmanaged_connection(Config, 0),
    true = is_pid(C091),
    C10 = open_amqp10_ok(Config),
    {refused, _} = open_amqp10(Config),
    {error, not_allowed} = rabbit_ct_client_helpers:open_unmanaged_connection(Config, 0),
    amqp_utils:close_connection_sync(C10),
    rabbit_ct_client_helpers:close_connection(C091),
    ok.

amqp10_connection_is_tracked(Config) ->
    ok = event_recorder:start(Config),
    try
        C = open_amqp10_ok(Config),
        ServerPid = wait_for_created_pid(Config),
        Local = rabbit_ct_broker_helpers:rpc(
                  Config, 0, rabbit_networking, local_connections, []),
        ?assert(lists:member(ServerPid, Local)),
        amqp_utils:close_connection_sync(C),
        wait_for_event_types(Config, [connection_closed])
    after
        ok = event_recorder:stop(Config)
    end,
    ok.

%% -------------------------------------------------------------------
%% Helpers
%% -------------------------------------------------------------------

set_node_limit(Config, Limit) ->
    ok = rabbit_ct_broker_helpers:rpc(Config, 0,
                                      application, set_env,
                                      [rabbit, connection_max, Limit]).

open_amqp10_ok(Config) ->
    {ok, Conn} = open_amqp10(Config),
    Conn.

open_amqp10(Config) ->
    {ok, Conn} = amqp10_client:open_connection(amqp_utils:connection_config(Config)),
    receive
        {amqp10_event, {connection, Conn, opened}} ->
            {ok, Conn};
        {amqp10_event, {connection, Conn, {closed, Reason}}} ->
            {refused, Reason}
    after ?TIMEOUT ->
              ct:fail({open_amqp10_timeout, Conn})
    end.

%% Covers the brief window between client-side close and Ranch
%% actually dropping the active TCP connection.
retry_open_amqp10(_Config, 0) ->
    ct:fail(retry_open_amqp10_exhausted);
retry_open_amqp10(Config, Attempts) ->
    case open_amqp10(Config) of
        {ok, _} = Result -> Result;
        {refused, _} ->
            timer:sleep(100),
            retry_open_amqp10(Config, Attempts - 1)
    end.

wait_for_created_pid(Config) ->
    wait_for_created_pid(Config, [], 30).

wait_for_created_pid(_Config, _Acc, 0) ->
    ct:fail(missing_connection_created_event);
wait_for_created_pid(Config, Acc, N) ->
    Acc1 = Acc ++ event_recorder:get_events(Config),
    case [proplists:get_value(pid, Props)
          || #event{type = connection_created, props = Props} <- Acc1] of
        [Pid | _] when is_pid(Pid) -> Pid;
        []                          -> wait_for_created_pid(Config, Acc1, N - 1)
    end.

wait_for_event_types(Config, Wanted) ->
    wait_for_event_types(Config, Wanted, 30).

wait_for_event_types(_Config, [], _N) ->
    ok;
wait_for_event_types(_Config, Wanted, 0) ->
    ct:fail({missing_event_types, Wanted});
wait_for_event_types(Config, Wanted, N) ->
    Seen = [T || #event{type = T} <- event_recorder:get_events(Config)],
    Remaining = [T || T <- Wanted, not lists:member(T, Seen)],
    case Remaining of
        [] -> ok;
        _  -> wait_for_event_types(Config, Remaining, N - 1)
    end.
