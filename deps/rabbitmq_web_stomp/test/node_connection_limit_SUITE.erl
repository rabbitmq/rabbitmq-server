%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

%% Verifies that `stomp.max_connections` covers Web STOMP alongside
%% native STOMP.

-module(node_connection_limit_SUITE).

-compile([export_all, nowarn_export_all]).

-include_lib("common_test/include/ct.hrl").
-include_lib("rabbit_common/include/rabbit.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("rabbitmq_ct_helpers/include/rabbit_assert.hrl").

-define(TIMEOUT, 5_000).

all() ->
    [cross_transport_native_and_web,
     cross_transport_count_decrements,
     web_stomp_connection_is_tracked].

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    Suffix = rabbit_ct_helpers:testcase_absname(Config, "", "-"),
    Config1 = rabbit_ct_helpers:set_config(Config, [
        {rmq_nodename_suffix, Suffix}
    ]),
    rabbit_ct_helpers:run_setup_steps(Config1,
        rabbit_ct_broker_helpers:setup_steps() ++
        rabbit_ct_client_helpers:setup_steps()).

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config,
        rabbit_ct_client_helpers:teardown_steps() ++
        rabbit_ct_broker_helpers:teardown_steps()).

init_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_started(Config, Testcase).

end_per_testcase(Testcase, Config) ->
    unset_max_connections(Config),
    await_local_count(Config, 0),
    rabbit_ct_helpers:testcase_finished(Config, Testcase).

cross_transport_native_and_web(Config) ->
    set_max_connections(Config, 2),
    {ok, Native} = open_native(Config),
    {ok, WS1} = open_web_stomp(Config),
    await_local_count(Config, 2),
    %% Third attempt over either transport is rejected — the slots used
    %% by the two existing connections count against the same limit.
    ?assertEqual({error, refused}, attempt_web_stomp(Config)),
    ?assertEqual({error, refused}, attempt_native(Config)),
    close_native(Native),
    close_web_stomp(WS1),
    await_local_count(Config, 0),
    ok.

cross_transport_count_decrements(Config) ->
    %% A slot freed by closing a Web STOMP connection must be reusable
    %% by a native STOMP connection.
    set_max_connections(Config, 1),
    {ok, WS1} = open_web_stomp(Config),
    ?assertEqual({error, refused}, attempt_native(Config)),
    close_web_stomp(WS1),
    await_local_count(Config, 0),
    {ok, Native} = open_native(Config),
    close_native(Native),
    await_local_count(Config, 0),
    ok.

web_stomp_connection_is_tracked(Config) ->
    {ok, WS} = open_web_stomp(Config),
    ?awaitMatch([_], tracked_stomp_connections(Config), 30_000),
    ok = rabbit_ct_broker_helpers:rpc(
           Config, 0, rabbit_connection_tracking, close_connections,
           [tracked_stomp_connections(Config), "closed by test", 0]),
    ?assertMatch({close, _}, rfc6455_client:recv(WS, ?TIMEOUT)),
    ?awaitMatch([], tracked_stomp_connections(Config), 30_000).

%% --- helpers ---

tracked_stomp_connections(Config) ->
    [C || #tracked_connection{protocol = {'STOMP', _}} = C
              <- rabbit_ct_broker_helpers:rpc(
                   Config, 0, rabbit_connection_tracking, list, [])].

set_max_connections(Config, Value) ->
    ok = rabbit_ct_broker_helpers:rpc(
           Config, 0, application, set_env,
           [rabbitmq_stomp, max_connections, Value]).

unset_max_connections(Config) ->
    ok = rabbit_ct_broker_helpers:rpc(
           Config, 0, application, unset_env,
           [rabbitmq_stomp, max_connections]).

count_local(Config) ->
    rabbit_ct_broker_helpers:rpc(
      Config, 0, rabbit_stomp, local_connection_count, []).

await_local_count(Config, Expected) ->
    ?awaitMatch(Expected, count_local(Config), 30_000).

%% --- native STOMP ---

open_native(Config) ->
    Port = rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_stomp),
    {ok, Sock} = gen_tcp:connect("localhost", Port,
                                 [binary, {active, false}, {packet, raw}]),
    ok = gen_tcp:send(Sock, stomp_connect_frame()),
    case gen_tcp:recv(Sock, 0, ?TIMEOUT) of
        {ok, <<"CONNECTED", _/binary>>} ->
            {ok, Sock};
        _ ->
            gen_tcp:close(Sock),
            {error, refused}
    end.

attempt_native(Config) ->
    case open_native(Config) of
        {ok, Sock} ->
            close_native(Sock),
            ok;
        _ ->
            {error, refused}
    end.

close_native(Sock) ->
    _ = (catch gen_tcp:send(Sock, <<"DISCONNECT\n\n\0">>)),
    _ = gen_tcp:close(Sock),
    ok.

stomp_connect_frame() ->
    <<"CONNECT\naccept-version:1.2\nhost:/\nlogin:guest\npasscode:guest\n\n\0">>.

%% --- Web STOMP (STOMP frames over a WebSocket) ---

open_web_stomp(Config) ->
    PortStr = integer_to_list(
                rabbit_ct_broker_helpers:get_node_config(
                  Config, 0, tcp_port_web_stomp)),
    WS = rfc6455_client:new("ws://127.0.0.1:" ++ PortStr ++ "/ws", self()),
    case rfc6455_client:open(WS) of
        {ok, _} ->
            ok = rfc6455_client:send(
                   WS, stomp:marshal("CONNECT",
                                     [{"accept-version", "1.2"},
                                      {"host", "/"},
                                      {"login", "guest"},
                                      {"passcode", "guest"}])),
            case rfc6455_client:recv(WS, ?TIMEOUT) of
                {ok, Frame} ->
                    case stomp:unmarshal(Frame) of
                        {<<"CONNECTED">>, _, _} ->
                            {ok, WS};
                        {<<"ERROR">>, _, _} ->
                            _ = rfc6455_client:close(WS),
                            {error, refused}
                    end;
                {close, _} ->
                    {error, refused}
            end;
        {close, _} ->
            {error, refused}
    end.

attempt_web_stomp(Config) ->
    case open_web_stomp(Config) of
        {ok, WS} ->
            close_web_stomp(WS),
            ok;
        Error ->
            Error
    end.

close_web_stomp(WS) ->
    _ = (catch rfc6455_client:close(WS)),
    ok.
