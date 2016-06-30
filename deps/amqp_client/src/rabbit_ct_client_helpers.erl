%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License
%% at http://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and
%% limitations under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2007-2015 Pivotal Software, Inc.  All rights reserved.
%%

-module(rabbit_ct_client_helpers).

-include_lib("common_test/include/ct.hrl").
-include("include/amqp_client.hrl").

-export([
    setup_steps/0,
    teardown_steps/0,
    start_channels_managers/1,
    stop_channels_managers/1,

    open_connection/2,
    open_unmanaged_connection/1, open_unmanaged_connection/2,
    close_connection/1,
    open_channel/2, open_channel/1,
    close_channel/1,
    open_connection_and_channel/2, open_connection_and_channel/1,
    close_connection_and_channel/2,
    close_channels_and_connection/2,

    publish/3, consume/3, fetch/3
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
            erlang:demonitor(MRef, [flush]),
            amqp_channel:close(Ch);
        false ->
            ok
    end,
    close_everything(Conn, Rest);
close_everything({Conn, MRef}, []) ->
    case erlang:is_process_alive(Conn) of
        true ->
            erlang:demonitor(MRef),
            amqp_connection:close(Conn);
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
    Port = rabbit_ct_broker_helpers:get_node_config(Config, Node,
      tcp_port_amqp),
    Params = #amqp_params_network{port = Port},
    {ok, Conn} = amqp_connection:start(Params),
    Conn.

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
         Exp = list_to_binary(integer_to_list(I)),
         receive {#'basic.deliver'{consumer_tag = CTag},
                  #amqp_msg{payload = Exp}} ->
                 ok
         after 500 ->
                 exit(timeout)
         end
     end|| I <- lists:seq(1, Count)],
    #'queue.declare_ok'{message_count = 0}
        = amqp_channel:call(Ch, #'queue.declare'{queue   = QName,
                                                 durable = true}),
    amqp_channel:call(Ch, #'basic.cancel'{consumer_tag = CTag}),
    ok.

fetch(Ch, QName, Count) ->
    [{#'basic.get_ok'{}, _} =
         amqp_channel:call(Ch, #'basic.get'{queue = QName}) ||
        _ <- lists:seq(1, Count)],
    ok.
