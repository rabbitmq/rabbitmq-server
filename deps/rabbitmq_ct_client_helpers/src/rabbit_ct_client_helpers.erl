%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_ct_client_helpers).

-include_lib("common_test/include/ct.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").

-export([
    setup_steps/0,
    teardown_steps/0,
    start_channels_managers/1,
    stop_channels_managers/1,

    open_connection/2,
    open_unmanaged_connection/1, open_unmanaged_connection/2,
    open_unmanaged_connection/3, open_unmanaged_connection/4, open_unmanaged_connection/5,
    open_unmanaged_connection_direct/1, open_unmanaged_connection_direct/2,
    open_unmanaged_connection_direct/3, open_unmanaged_connection_direct/4,
    open_unmanaged_connection_direct/5,
    open_unmanaged_connection/6,
    close_connection/1, open_channel/2, open_channel/1,
    close_channel/1,
    open_connection_and_channel/2, open_connection_and_channel/1,
    close_connection_and_channel/2,
    close_channels_and_connection/2,

    publish/3, consume/3, consume_without_acknowledging/3, fetch/3
  ]).

%% -------------------------------------------------------------------
%% Client setup/teardown steps.
%% -------------------------------------------------------------------

setup_steps() ->
    [
      fun start_channels_managers/1
    ].

teardown_steps() ->
    [
      fun stop_channels_managers/1
    ].

start_channels_managers(Config) ->
    ok = application:set_env(amqp_client, gen_server_call_timeout, infinity),
    NodeConfigs = rabbit_ct_broker_helpers:get_node_configs(Config),
    NodeConfigs1 = [start_channels_manager(NC) || NC <- NodeConfigs],
    rabbit_ct_helpers:set_config(Config, {rmq_nodes, NodeConfigs1}).

start_channels_manager(NodeConfig) ->
    Pid = erlang:spawn(
      fun() -> channels_manager(NodeConfig, undefined, []) end),
    rabbit_ct_helpers:set_config(NodeConfig, {channels_manager, Pid}).

stop_channels_managers(Config) ->
    NodeConfigs = rabbit_ct_broker_helpers:get_node_configs(Config),
    NodeConfigs1 = [stop_channels_manager(NC) || NC <- NodeConfigs],
    rabbit_ct_helpers:set_config(Config, {rmq_nodes, NodeConfigs1}).

stop_channels_manager(NodeConfig) ->
    Pid = ?config(channels_manager, NodeConfig),
    Pid ! stop,
    proplists:delete(channels_manager, NodeConfig).

channels_manager(NodeConfig, ConnTuple, Channels) ->
    receive
        {open_connection, From} ->
            {Conn1, _} = ConnTuple1 = open_conn(NodeConfig, ConnTuple),
            From ! Conn1,
            channels_manager(NodeConfig, ConnTuple1, Channels);
        {open_channel, From} ->
            {Conn1, _} = ConnTuple1 = open_conn(NodeConfig, ConnTuple),
            {ok, Ch} = amqp_connection:open_channel(Conn1),
            ChMRef = erlang:monitor(process, Ch),
            From ! Ch,
            channels_manager(NodeConfig, ConnTuple1,
              [{Ch, ChMRef} | Channels]);
        {close_everything, From} ->
            close_everything(ConnTuple, Channels),
            From ! ok,
            channels_manager(NodeConfig, undefined, []);
        {'DOWN', ConnMRef, process, Conn, _}
        when {Conn, ConnMRef} =:= ConnTuple ->
            channels_manager(NodeConfig, undefined, Channels);
        {'DOWN', ChMRef, process, Ch, _} ->
            Channels1 = Channels -- [{Ch, ChMRef}],
            channels_manager(NodeConfig, ConnTuple, Channels1);
        stop ->
            close_everything(ConnTuple, Channels);
        Unhandled ->
            ct:pal(?LOW_IMPORTANCE,
              "Channels manager ~p: unhandled message: ~p",
              [self(), Unhandled]),
            channels_manager(NodeConfig, ConnTuple, Channels)
    end.

open_conn(NodeConfig, undefined) ->
    Port = ?config(tcp_port_amqp, NodeConfig),
    Params = #amqp_params_network{port = Port},
    {ok, Conn} = amqp_connection:start(Params),
    MRef = erlang:monitor(process, Conn),
    {Conn, MRef};
open_conn(NodeConfig, {Conn, _} = ConnTuple) ->
    case erlang:is_process_alive(Conn) of
        true  -> ConnTuple;
        false -> open_conn(NodeConfig, undefined)
    end.

close_everything(Conn, [{Ch, MRef} | Rest]) ->
    case erlang:is_process_alive(Ch) of
        true ->
            amqp_channel:close(Ch),
            receive
                {'DOWN', MRef, _, Ch, Info} ->
                    ct:pal("Channel ~p closed: ~p~n", [Ch, Info])
            end;
        false ->
            ok
    end,
    close_everything(Conn, Rest);
close_everything({Conn, MRef}, []) ->
    case erlang:is_process_alive(Conn) of
        true ->
            amqp_connection:close(Conn),
            receive
                {'DOWN', MRef, _, Conn, Info} ->
                    ct:pal("Connection ~p closed: ~p~n", [Conn, Info])
            end;
        false ->
            ok
    end;
close_everything(undefined, []) ->
    ok.

%% -------------------------------------------------------------------
%% Public API.
%% -------------------------------------------------------------------

open_connection(Config, Node) ->
    Pid = rabbit_ct_broker_helpers:get_node_config(Config, Node,
      channels_manager),
    Pid ! {open_connection, self()},
    receive
        Conn when is_pid(Conn) -> Conn
    end.

open_unmanaged_connection(Config) ->
    open_unmanaged_connection(Config, 0).

open_unmanaged_connection(Config, Node) ->
    open_unmanaged_connection(Config, Node, ?config(rmq_vhost, Config)).

open_unmanaged_connection(Config, Node, VHost) ->
    open_unmanaged_connection(Config, Node, VHost,
      ?config(rmq_username, Config), ?config(rmq_password, Config)).

open_unmanaged_connection(Config, Node, Username, Password) ->
    open_unmanaged_connection(Config, Node, ?config(rmq_vhost, Config),
      Username, Password).

open_unmanaged_connection(Config, Node, VHost, Username, Password) ->
    open_unmanaged_connection(Config, Node, VHost, Username, Password,
      network).

open_unmanaged_connection_direct(Config) ->
    open_unmanaged_connection_direct(Config, 0).

open_unmanaged_connection_direct(Config, Node) ->
    open_unmanaged_connection_direct(Config, Node, ?config(rmq_vhost, Config)).

open_unmanaged_connection_direct(Config, Node, VHost) ->
    open_unmanaged_connection_direct(Config, Node, VHost,
      ?config(rmq_username, Config), ?config(rmq_password, Config)).

open_unmanaged_connection_direct(Config, Node, Username, Password) ->
    open_unmanaged_connection_direct(Config, Node, ?config(rmq_vhost, Config),
      Username, Password).

open_unmanaged_connection_direct(Config, Node, VHost, Username, Password) ->
    open_unmanaged_connection(Config, Node, VHost, Username, Password, direct).

open_unmanaged_connection(Config, Node, VHost, Username, Password, Type) ->
    Params = case Type of
        network ->
            Port = rabbit_ct_broker_helpers:get_node_config(Config, Node,
                                                            tcp_port_amqp),
            #amqp_params_network{port = Port,
                                 virtual_host = VHost,
                                 username = Username,
                                 password = Password};
        direct ->
            NodeName = rabbit_ct_broker_helpers:get_node_config(Config, Node,
                                                               nodename),
            #amqp_params_direct{node = NodeName,
                                virtual_host = VHost,
                                username = Username,
                                password = Password}
    end,
    case amqp_connection:start(Params) of
        {ok, Conn}         -> Conn;
        {error, _} = Error -> Error
    end.

open_channel(Config) ->
    open_channel(Config, 0).

open_channel(Config, Node) ->
    Pid = rabbit_ct_broker_helpers:get_node_config(Config, Node,
      channels_manager),
    Pid ! {open_channel, self()},
    receive
        Ch when is_pid(Ch) -> Ch
    end.

open_connection_and_channel(Config) ->
    open_connection_and_channel(Config, 0).

open_connection_and_channel(Config, Node) ->
    Conn = open_connection(Config, Node),
    Ch = open_channel(Config, Node),
    {Conn, Ch}.

close_channel(Ch) ->
    case is_process_alive(Ch) of
        true  -> amqp_channel:close(Ch);
        false -> ok
    end.

close_connection(Conn) ->
    case is_process_alive(Conn) of
        true  -> amqp_connection:close(Conn);
        false -> ok
    end.

close_connection_and_channel(Conn, Ch) ->
    _ = close_channel(Ch),
    case close_connection(Conn) of
        ok      -> ok;
        closing -> ok
    end.

close_channels_and_connection(Config, Node) ->
    Pid = rabbit_ct_broker_helpers:get_node_config(Config, Node,
      channels_manager),
    Pid ! {close_everything, self()},
    receive
        ok -> ok
    end.

publish(Ch, QName, Count) ->
    amqp_channel:call(Ch, #'confirm.select'{}),
    [amqp_channel:call(Ch,
                       #'basic.publish'{routing_key = QName},
                       #amqp_msg{props   = #'P_basic'{delivery_mode = 2},
                                 payload = list_to_binary(integer_to_list(I))})
     || I <- lists:seq(1, Count)],
    amqp_channel:wait_for_confirms(Ch).

consume(Ch, QName, Count) ->
    amqp_channel:subscribe(Ch, #'basic.consume'{queue = QName, no_ack = true},
                           self()),
    CTag = receive #'basic.consume_ok'{consumer_tag = C} -> C end,
    [begin
         Exp = integer_to_binary(I),
         receive {#'basic.deliver'{consumer_tag = CTag},
                  #amqp_msg{payload = Exp}} ->
                 ok
         after 5000 ->
                 exit(timeout)
         end
     end || I <- lists:seq(1, Count)],
    amqp_channel:call(Ch, #'basic.cancel'{consumer_tag = CTag}),
    ok.

consume_without_acknowledging(Ch, QName, Count) ->
    amqp_channel:subscribe(Ch, #'basic.consume'{queue = QName, no_ack = false},
                           self()),
    CTag = receive #'basic.consume_ok'{consumer_tag = C} -> C end,
    accumulate_without_acknowledging(Ch, CTag, Count, []).

accumulate_without_acknowledging(Ch, CTag, Remaining, Acc) when Remaining =:= 0 ->
    amqp_channel:call(Ch, #'basic.cancel'{consumer_tag = CTag}),
    lists:reverse(Acc);
accumulate_without_acknowledging(Ch, CTag, Remaining, Acc) ->
    receive {#'basic.deliver'{consumer_tag = CTag, delivery_tag = DTag}, _Msg} ->
           accumulate_without_acknowledging(Ch, CTag, Remaining - 1, [DTag | Acc])
    after 5000 ->
           amqp_channel:call(Ch, #'basic.cancel'{consumer_tag = CTag}),
           exit(timeout)
    end.


fetch(Ch, QName, Count) ->
    [{#'basic.get_ok'{}, _} =
         amqp_channel:call(Ch, #'basic.get'{queue = QName}) ||
        _ <- lists:seq(1, Count)],
    ok.
