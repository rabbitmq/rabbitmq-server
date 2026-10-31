%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(unit_connection_tracking_SUITE).

-include_lib("eunit/include/eunit.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("kernel/include/file.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("rabbitmq_ct_helpers/include/rabbit_assert.hrl").

-compile(export_all).

-define(TIMEOUT, 30000).

all() ->
    [
      {group, non_parallel_tests}
    ].

groups() ->
    [
      {non_parallel_tests, [], [
          exchange_count,
          queue_count,
          connection_count,
          connection_lookup,
          connection_tracking_event_churn,
          channel_tracking_event_churn,
          tracking_handlers_survive_errors,
          channels_shut_down_only_for_amqpl_connections,
          amqpl_connection_churn,
          amqp_connection_churn
        ]}
    ].

%% -------------------------------------------------------------------
%% Test suite setup/teardown.
%% -------------------------------------------------------------------

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(amqp10_client),
    rabbit_ct_helpers:log_environment(),
    rabbit_ct_helpers:run_setup_steps(Config).

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config).

init_per_group(Group, Config) ->
    Config1 = rabbit_ct_helpers:set_config(Config, [
        {rmq_nodename_suffix, Group},
        {rmq_nodes_count, 1}
      ]),
    rabbit_ct_helpers:run_steps(Config1,
      rabbit_ct_broker_helpers:setup_steps() ++
      rabbit_ct_client_helpers:setup_steps()).

end_per_group(_Group, Config) ->
    rabbit_ct_helpers:run_steps(Config,
      rabbit_ct_client_helpers:teardown_steps() ++
      rabbit_ct_broker_helpers:teardown_steps()).

init_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_started(Config, Testcase).

end_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_finished(Config, Testcase).


%% ---------------------------------------------------------------------------
%% Count functions for management only API purposes
%% ---------------------------------------------------------------------------

exchange_count(Config) ->
    %% Default exchanges == 8
    ?assertEqual(8, rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_exchange, count, [])).

queue_count(Config) ->
    Conn = rabbit_ct_client_helpers:open_connection(Config, 0),
    {ok, Ch} = amqp_connection:open_channel(Conn),
    amqp_channel:call(Ch, #'queue.declare'{ queue = <<"my-queue">>, durable = true }),

    ?assertEqual(1, rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_amqqueue, count, [])),

    amqp_channel:call(Ch, #'queue.delete'{ queue = <<"my-queue">> }),
    rabbit_ct_client_helpers:close_channel(Ch),
    rabbit_ct_client_helpers:close_connection(Conn),
    ok.

%% connection_count/1 has been failing on Travis. This seems a legit failure, as the registering
%% of connections in the tracker is async. `rabbit_connection_tracking_handler` receives a rabbit
%% event with `connection_created`, which then forwards as a cast to `rabbit_connection_tracker`
%% for register. We should wait a reasonable amount of time for the counter to increase before
%% failing.
connection_count(Config) ->
    Conn = rabbit_ct_client_helpers:open_connection(Config, 0),

    rabbit_ct_helpers:await_condition(
      fun() ->
              rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_connection_tracking, count, []) == 1
      end, 30000),

    rabbit_ct_client_helpers:close_connection(Conn),
    ok.

connection_lookup(Config) ->
    ConnsBefore = rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_connection_tracking, list, []),
    Conn = rabbit_ct_client_helpers:open_connection(Config, 0),

    %% Let's wait until the connection is registered, otherwise this test could fail in a slow
    %% machine as connection tracking is asynchronous
    rabbit_ct_helpers:await_condition(
      fun() ->
              length(rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_connection_tracking, list, [])
                     -- ConnsBefore) == 1
      end, 30000),

    [Connection] = rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_connection_tracking, list, []) -- ConnsBefore,
    ?assertMatch(Connection, rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_connection_tracking,
                                                          lookup,
                                                          [Connection#tracked_connection.name])),

    rabbit_ct_client_helpers:close_connection(Conn),
    ok.

%% ---------------------------------------------------------------------------
%% Connection and channel churn
%% ---------------------------------------------------------------------------

-define(CHURN_WORKERS, 20).
-define(CHURN_ITERATIONS, 50).
-define(EVENT_CHURN, 10_000).

connection_tracking_event_churn(Config) ->
    Name = atom_to_binary(?FUNCTION_NAME),
    Created = fun(ConnName) ->
                      [{name, ConnName}, {node, node_name(Config)}, {pid, self()},
                       {vhost, Name}, {user, Name}, {protocol, {0,9,1}},
                       {type, network}, {connected_at, os:system_time(millisecond)}]
              end,
    Closed = fun(ConnName) ->
                     [{name, ConnName}, {node, node_name(Config)}, {pid, self()}]
             end,
    Events = lists:append([[{connection_created, Created(N)},
                            {connection_closed, Closed(N)}]
                           || I <- lists:seq(1, ?EVENT_CHURN),
                              N <- [integer_to_binary(I)]]),
    ok = notify(Config, Events ++ [{connection_created, Created(<<"open">>)}]),
    ?awaitMatch({1, 1, [#tracked_connection{name = <<"open">>}]},
                tracked_connections(Config, Name, Name),
                ?TIMEOUT),
    ok = notify(Config, [{connection_closed, Closed(<<"open">>)}]),
    ?awaitMatch({0, 0, []}, tracked_connections(Config, Name, Name), ?TIMEOUT).

channel_tracking_event_churn(Config) ->
    User = atom_to_binary(?FUNCTION_NAME),
    [OpenChPid | ChPids] = rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, dead_pids,
                                                        [?EVENT_CHURN + 1]),
    Created = fun(ChPid) ->
                      [{pid, ChPid}, {name, list_to_binary(pid_to_list(ChPid))},
                       {vhost, <<"/">>}, {connection, self()}, {user, User}]
              end,
    Events = lists:append([[{channel_created, Created(ChPid)},
                            {channel_closed, [{pid, ChPid}]}]
                           || ChPid <- ChPids]),
    ok = notify(Config, Events ++ [{channel_created, Created(OpenChPid)}]),
    ?awaitMatch({1, [#tracked_channel{pid = OpenChPid}]},
                tracked_channels(Config, User),
                ?TIMEOUT),
    ok = notify(Config, [{channel_closed, [{pid, OpenChPid}]}]),
    ?awaitMatch({0, []}, tracked_channels(Config, User), ?TIMEOUT).

tracking_handlers_survive_errors(Config) ->
    Name = atom_to_binary(?FUNCTION_NAME),
    Node = node_name(Config),
    [ChPid] = rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, dead_pids, [1]),
    ok = notify(Config, [{connection_created,
                          [{name, Name}, {node, Node}, {pid, self()},
                           {vhost, Name}, {user, Name}, {type, network}]},
                         {channel_created,
                          [{pid, ChPid}, {name, Name}, {vhost, Name},
                           {connection, self()}, {user, Name}]}]),
    ?awaitMatch({1, 1, [_]}, tracked_connections(Config, Name, Name), ?TIMEOUT),
    ?awaitMatch({1, [_]}, tracked_channels(Config, Name), ?TIMEOUT),
    %% As if the user had been deleted while the connection was open.
    [true = rabbit_ct_broker_helpers:rpc(Config, 0, ets, delete, [Tab, Name])
     || Tab <- [tracked_connection_per_user, tracked_channel_per_user]],
    ok = notify(Config, [{channel_closed, [{pid, ChPid}]},
                         {connection_closed, [{name, Name}, {node, Node}, {pid, self()}]},
                         {channel_created, []},
                         {channel_closed, [{pid, self()}]}]),
    ?awaitMatch({0, 0, []}, tracked_connections(Config, Name, Name), ?TIMEOUT),
    ?awaitMatch({0, []}, tracked_channels(Config, Name), ?TIMEOUT),
    [ok = rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_event, sync_notify,
                                        [Type, invalid])
     || Type <- [connection_created, connection_closed]],
    Handlers = rabbit_ct_broker_helpers:rpc(Config, 0, gen_event, which_handlers,
                                            [rabbit_event]),
    ?assert(lists:member(rabbit_connection_tracking_handler, Handlers)),
    ?assert(lists:member(rabbit_channel_tracking_handler, Handlers)),
    {Conn, _Ch} = rabbit_ct_client_helpers:open_connection_and_channel(Config, 0),
    ?awaitMatch({1, 1, [_]}, tracked_connections(Config, <<"/">>, <<"guest">>), ?TIMEOUT),
    ?awaitMatch({1, [_]}, tracked_channels(Config, <<"guest">>), ?TIMEOUT),
    ok = rabbit_ct_client_helpers:close_connection(Conn),
    ?awaitMatch({0, 0, []}, tracked_connections(Config, <<"/">>, <<"guest">>), ?TIMEOUT).

channels_shut_down_only_for_amqpl_connections(Config) ->
    [?assertEqual({Protocol, Expected}, {Protocol, channel_shut_down(Config, Protocol, Type)})
     || {Protocol, Type, Expected} <- [{{0, 9, 1}, network, true},
                                       {{'Direct', {0, 9, 1}}, direct, true},
                                       {{1, 0}, network, false},
                                       {{'MQTT', "3.1.1"}, network, false},
                                       {{'STOMP', "1.2"}, network, false},
                                       {<<"stream">>, network, false}]].

channel_shut_down(Config, Protocol, Type) ->
    Name = iolist_to_binary(io_lib:format("~0p", [Protocol])),
    Node = node_name(Config),
    ChPid = rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, start_recorder, []),
    ok = notify(Config, [{connection_created,
                          [{name, Name}, {node, Node}, {pid, self()}, {vhost, Name},
                           {user, Name}, {protocol, Protocol}, {type, Type}]},
                         {channel_created,
                          [{pid, ChPid}, {name, Name}, {vhost, Name},
                           {connection, self()}, {user, Name}]},
                         {connection_closed, [{name, Name}, {node, Node}, {pid, self()}]}]),
    ok = rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, await_channel_tracking, []),
    ShutDown = case Type =:= direct orelse Protocol =:= {0, 9, 1} of
                   true ->
                       ?awaitMatch([{'$gen_cast', terminate}], recorded(ChPid), ?TIMEOUT),
                       true;
                   false ->
                       recorded(ChPid) =/= []
               end,
    ok = notify(Config, [{channel_closed, [{pid, ChPid}]}]),
    ?awaitMatch({0, []}, tracked_channels(Config, Name), ?TIMEOUT),
    ShutDown.

await_channel_tracking() ->
    ok = rabbit_event:sync_notify(?FUNCTION_NAME, []),
    Busy = [P || P <- processes(),
                 process_info(P, initial_call) =:=
                     {initial_call, {rabbit_channel_tracking, handle_cast, 1}}],
    case Busy of
        [] -> ok;
        _ -> timer:sleep(10), await_channel_tracking()
    end.

start_recorder() ->
    spawn(fun() -> recorder([]) end).

recorder(Msgs) ->
    receive
        {get, From} -> From ! {recorded, lists:reverse(Msgs)}, recorder(Msgs);
        Msg -> recorder([Msg | Msgs])
    end.

recorded(Pid) ->
    Pid ! {get, self()},
    receive {recorded, Msgs} -> Msgs
    after ?TIMEOUT -> ct:fail(recorder_timeout)
    end.

amqpl_connection_churn(Config) ->
    Port = rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_amqp),
    Frame = fun(Method) -> rabbit_binary_generator:build_simple_method_frame(0, Method) end,
    %% The handshake and the close are pipelined, so that the connection
    %% closes as soon as it has been opened.
    Frames = [<<"AMQP", 0, 0, 9, 1>>,
              Frame(#'connection.start_ok'{client_properties = [],
                                           mechanism = <<"PLAIN">>,
                                           response = <<0, "guest", 0, "guest">>,
                                           locale = <<"en_US">>}),
              Frame(#'connection.tune_ok'{channel_max = 16,
                                          frame_max = 131072,
                                          heartbeat = 0}),
              Frame(#'connection.open'{virtual_host = <<"/">>}),
              Frame(#'connection.close'{reply_code = 200,
                                        reply_text = <<>>,
                                        class_id = 0,
                                        method_id = 0})],
    CloseOk = iolist_to_binary(Frame(#'connection.close_ok'{})),
    churn(Config,
          fun() ->
                  {ok, Sock} = gen_tcp:connect("localhost", Port,
                                               [binary, {active, false}, {nodelay, true}]),
                  ok = gen_tcp:send(Sock, Frames),
                  ok = recv_until(Sock, CloseOk, <<>>),
                  ok = gen_tcp:close(Sock)
          end).

amqp_connection_churn(Config) ->
    OpnConf = #{address => ?config(rmq_hostname, Config),
                port => rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_amqp),
                container_id => atom_to_binary(?FUNCTION_NAME),
                sasl => {plain, <<"guest">>, <<"guest">>}},
    churn(Config,
          fun() ->
                  {ok, C} = amqp10_client:open_connection(OpnConf),
                  receive {amqp10_event, {connection, C, opened}} -> ok
                  after ?TIMEOUT -> ct:fail(opened_timeout)
                  end,
                  ok = amqp10_client:close_connection(C),
                  receive {amqp10_event, {connection, C, {closed, normal}}} -> ok
                  after ?TIMEOUT -> ct:fail(closed_timeout)
                  end
          end).

churn(Config, Fun) ->
    {Conn, _Ch} = rabbit_ct_client_helpers:open_connection_and_channel(Config, 0),
    Workers = [spawn_monitor(fun() ->
                                     [Fun() || _ <- lists:seq(1, ?CHURN_ITERATIONS)]
                             end)
               || _ <- lists:seq(1, ?CHURN_WORKERS)],
    [receive {'DOWN', Ref, process, Pid, Reason} -> ?assertEqual(normal, Reason)
     end || {Pid, Ref} <- Workers],
    ?awaitMatch({1, 1, [_]}, tracked_connections(Config, <<"/">>, <<"guest">>), ?TIMEOUT),
    ?awaitMatch({1, [_]}, tracked_channels(Config, <<"guest">>), ?TIMEOUT),
    ok = rabbit_ct_client_helpers:close_connection(Conn),
    ?awaitMatch({0, 0, []}, tracked_connections(Config, <<"/">>, <<"guest">>), ?TIMEOUT),
    ?awaitMatch({0, []}, tracked_channels(Config, <<"guest">>), ?TIMEOUT).

recv_until(Sock, Pattern, Acc0) ->
    {ok, Data} = gen_tcp:recv(Sock, 0, ?TIMEOUT),
    Acc = <<Acc0/binary, Data/binary>>,
    case binary:match(Acc, Pattern) of
        nomatch -> recv_until(Sock, Pattern, Acc);
        _ -> ok
    end.

node_name(Config) ->
    rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename).

%% Events are emitted by a single process, in order, as a connection or
%% a channel would emit them.
notify(Config, Events) ->
    rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, notify, [Events]).

notify(Events) ->
    lists:foreach(fun({Type, Props}) -> rabbit_event:notify(Type, Props) end, Events).

dead_pids(N) ->
    [spawn(fun() -> ok end) || _ <- lists:seq(1, N)].

tracked_connections(Config, VHost, User) ->
    {rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_connection_tracking,
                                  count_tracked_items_in, [{vhost, VHost}]),
     rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_connection_tracking,
                                  count_tracked_items_in, [{user, User}]),
     rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_connection_tracking,
                                  list_of_user, [User])}.

tracked_channels(Config, User) ->
    {rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_channel_tracking,
                                  count_tracked_items_in, [{user, User}]),
     rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_channel_tracking,
                                  list_of_user, [User])}.
