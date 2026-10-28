%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(resource_alarm_SUITE).
-compile([export_all, nowarn_export_all]).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

suite() ->
    [{timetrap, {minutes, 2}}].

all() ->
    [memory_alarm, disk_alarm, existing_alarms, overlapping_alarms,
     alarm_cleared_before_publish, heartbeats_started_after_blocking,
     blocked_connection_idle_timeout, idle_timeout_before_publish,
     consumer_receives_during_alarm].

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    {ok, _} = application:ensure_all_started(gun),
    Config1 = rabbit_ct_helpers:set_config(
                Config, [{rmq_nodename_suffix, ?MODULE}]),
    rabbit_ct_helpers:run_setup_steps(
      Config1, rabbit_ct_broker_helpers:setup_steps()).

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(
      Config, rabbit_ct_broker_helpers:teardown_steps()).

init_per_testcase(Testcase, Config)
  when Testcase =:= blocked_connection_idle_timeout;
       Testcase =:= idle_timeout_before_publish ->
    WsOpts = rpc(Config, application, get_env,
                 [rabbitmq_web_stomp, cowboy_ws_opts, []]),
    ok = rabbit_ws_test_util:update_app_env(
           Config, cowboy_ws_opts, [{idle_timeout, 1000} |
                                    proplists:delete(idle_timeout, WsOpts)]),
    [{saved_ws_opts, WsOpts} | Config];
init_per_testcase(_, Config) ->
    Config.

end_per_testcase(_, Config) ->
    clear_alarm(Config, memory),
    clear_alarm(Config, disk),
    case proplists:lookup(saved_ws_opts, Config) of
        {saved_ws_opts, WsOpts} ->
            ok = rabbit_ws_test_util:update_app_env(Config, cowboy_ws_opts, WsOpts);
        none ->
            ok
    end,
    Config.

memory_alarm(Config) ->
    alarm_blocks_publisher(Config, memory).

disk_alarm(Config) ->
    alarm_blocks_publisher(Config, disk).

alarm_blocks_publisher(Config, Source) ->
    with_connection(Config, fun(Conn, Ref, Pid) ->
        set_alarm(Config, Source),
        await_state(Config, Pid, blocking),
        block_publisher(Config, Conn, Ref, Pid),
        clear_alarm(Config, Source),
        await_state(Config, Pid, running),
        await_receipt(Conn, Ref, <<"pending">>)
    end).

existing_alarms(Config) ->
    set_alarm(Config, memory),
    set_alarm(Config, disk),
    with_connection(Config, fun(Conn, Ref, Pid) ->
        await_state(Config, Pid, blocking),
        block_publisher(Config, Conn, Ref, Pid),
        clear_alarm(Config, memory),
        ?assertEqual({error, timeout}, gun:await(Conn, Ref, 200)),
        await_state(Config, Pid, blocked),
        clear_alarm(Config, disk),
        await_state(Config, Pid, running),
        await_receipt(Conn, Ref, <<"pending">>)
    end).

overlapping_alarms(Config) ->
    with_connection(Config, fun(Conn, Ref, Pid) ->
        set_alarm(Config, memory),
        set_alarm(Config, disk),
        await_state(Config, Pid, blocking),
        block_publisher(Config, Conn, Ref, Pid),
        clear_alarm(Config, memory),
        ?assertEqual({error, timeout}, gun:await(Conn, Ref, 200)),
        await_state(Config, Pid, blocked),
        clear_alarm(Config, disk),
        await_state(Config, Pid, running),
        await_receipt(Conn, Ref, <<"pending">>)
    end).

alarm_cleared_before_publish(Config) ->
    with_connection(Config, [{"heart-beat", "10000,0"}], <<>>,
                    fun(Conn, Ref, Pid) ->
        set_alarm(Config, memory),
        await_state(Config, Pid, blocking),
        clear_alarm(Config, memory),
        await_state(Config, Pid, running),
        ?assertEqual({error, timeout}, gun:await(Conn, Ref, 200)),
        send(Conn, Ref, "SUBSCRIBE",
             [{"destination", "/topic/resource-alarm"}, {"id", "s0"},
              {"receipt", "subscribed"}], <<>>),
        await_receipt(Conn, Ref, <<"subscribed">>)
    end).

heartbeats_started_after_blocking(Config) ->
    set_alarm(Config, memory),
    Send = stomp:marshal("SEND", [{"destination", "/topic/resource-alarm"}]),
    with_connection(Config, [{"heart-beat", "1000,0"}], Send,
                    fun(Conn, Ref, Pid) ->
        await_state(Config, Pid, blocked),
        ?assertEqual({error, timeout}, gun:await(Conn, Ref, 3500)),
        clear_alarm(Config, memory),
        await_state(Config, Pid, running),
        send(Conn, Ref, "SUBSCRIBE",
             [{"destination", "/topic/resource-alarm"}, {"id", "s0"},
              {"receipt", "subscribed"}], <<>>),
        await_receipt(Conn, Ref, <<"subscribed">>),
        ?assertEqual({ws, {close, 1000, <<"STOMP died">>}},
                     gun:await(Conn, Ref, 4000))
    end).

blocked_connection_idle_timeout(Config) ->
    with_connection(Config, [{"heart-beat", "10000,0"}], <<>>,
                    fun(Conn, Ref, Pid) ->
        {Sender, Monitor} = spawn_monitor(fun() -> send_heartbeats(Conn, Ref) end),
        try
            ?assertEqual({error, timeout}, gun:await(Conn, Ref, 1500)),
            set_alarm(Config, memory),
            await_state(Config, Pid, blocking),
            block_publisher(Config, Conn, Ref, Pid),
            ?assertEqual({error, timeout}, gun:await(Conn, Ref, 2500)),
            clear_alarm(Config, memory),
            await_state(Config, Pid, running),
            await_receipt(Conn, Ref, <<"pending">>),
            Sender ! stop,
            receive
                {'DOWN', Monitor, process, Sender, normal} -> ok
            after 1000 ->
                ct:fail(heartbeat_sender_did_not_stop)
            end,
            ?assertEqual({ws, {close, 1000, <<>>}}, gun:await(Conn, Ref, 3000))
        after
            Sender ! stop
        end
    end).

idle_timeout_before_publish(Config) ->
    with_connection(Config, fun(Conn, Ref, Pid) ->
        set_alarm(Config, memory),
        await_state(Config, Pid, blocking),
        ?assertEqual({ws, {close, 1000, <<>>}}, gun:await(Conn, Ref, 3000))
    end).

send_heartbeats(Conn, Ref) ->
    receive
        stop -> ok
    after 100 ->
        gun:ws_send(Conn, Ref, {text, <<"\n">>}),
        send_heartbeats(Conn, Ref)
    end.

consumer_receives_during_alarm(Config) ->
    with_connection(Config, fun(Consumer, ConsumerRef, ConsumerPid) ->
        send(Consumer, ConsumerRef, "SUBSCRIBE",
             [{"destination", "/topic/resource-alarm"}, {"id", "s0"},
              {"receipt", "subscribed"}], <<>>),
        await_receipt(Consumer, ConsumerRef, <<"subscribed">>),
        set_alarm(Config, memory),
        await_state(Config, ConsumerPid, blocking),
        with_connection(Config, fun(Publisher, PublisherRef, PublisherPid) ->
            await_state(Config, PublisherPid, blocking),
            send(Publisher, PublisherRef, "SEND",
                 [{"destination", "/topic/resource-alarm"}], <<"message">>),
            {<<"MESSAGE">>, _, <<"message">>} = recv(Consumer, ConsumerRef),
            await_state(Config, PublisherPid, blocked),
            await_state(Config, ConsumerPid, blocking)
        end)
    end).

block_publisher(Config, Conn, Ref, Pid) ->
    send(Conn, Ref, "SEND", [{"destination", "/topic/resource-alarm"}], <<>>),
    await_state(Config, Pid, blocked),
    send(Conn, Ref, "SUBSCRIBE",
         [{"destination", "/topic/resource-alarm"}, {"id", "pending"},
          {"receipt", "pending"}], <<>>),
    ?assertEqual({error, timeout}, gun:await(Conn, Ref, 200)).

with_connection(Config, Fun) ->
    with_connection(Config, [], <<>>, Fun).

with_connection(Config, ConnectHeaders, InitialData, Fun) ->
    Before = rpc(Config, ets, tab2list, [connection_created]),
    Port = rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_web_stomp),
    {ok, Conn} = gun:open("localhost", Port, #{protocols => [http]}),
    try
        {ok, http} = gun:await_up(Conn),
        Ref = gun:ws_upgrade(Conn, "/ws"),
        {upgrade, [<<"websocket">>], _} = gun:await(Conn, Ref),
        Connect = stomp:marshal("CONNECT", [{"login", "guest"}, {"passcode", "guest"} |
                                           ConnectHeaders]),
        gun:ws_send(Conn, Ref, {text, <<Connect/binary, InitialData/binary>>}),
        {<<"CONNECTED">>, _, <<>>} = recv(Conn, Ref),
        rabbit_ct_helpers:await_condition(fun() ->
            length(rpc(Config, ets, tab2list, [connection_created]) -- Before) =:= 1
        end, 5000),
        [{Pid, _}] = rpc(Config, ets, tab2list, [connection_created]) -- Before,
        Fun(Conn, Ref, Pid)
    after
        gun:close(Conn)
    end.

send(Conn, Ref, Command, Headers, Body) ->
    gun:ws_send(Conn, Ref, {text, stomp:marshal(Command, Headers, Body)}).

recv(Conn, Ref) ->
    {ws, {text, Frame}} = gun:await(Conn, Ref),
    stomp:unmarshal(Frame).

await_receipt(Conn, Ref, Id) ->
    {<<"RECEIPT">>, Headers, <<>>} = recv(Conn, Ref),
    ?assertEqual(Id, proplists:get_value(<<"receipt-id">>, Headers)).

await_state(Config, Pid, Expected) ->
    rabbit_ct_helpers:await_condition(fun() ->
        rpc(Config, erlang, send, [Pid, emit_stats]),
        case rpc(Config, ets, lookup, [connection_metrics, Pid]) of
            [{Pid, Infos}] -> proplists:get_value(state, Infos) =:= Expected;
            [] -> false
        end
    end, 5000).

set_alarm(Config, Source) ->
    Node = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),
    ok = rpc(Config, rabbit_alarm, set_alarm, [{{resource_limit, Source, Node}, []}]),
    rpc(Config, rabbit_alarm, get_alarms, []).

clear_alarm(Config, Source) ->
    Node = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),
    ok = rpc(Config, rabbit_alarm, clear_alarm, [{resource_limit, Source, Node}]),
    rpc(Config, rabbit_alarm, get_alarms, []).

rpc(Config, Module, Function, Args) ->
    rabbit_ct_broker_helpers:rpc(Config, 0, Module, Function, Args).
