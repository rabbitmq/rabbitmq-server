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

    open_connection/2, close_connection/1,
    open_channel/2, close_channel/1
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
        {'DOWN', ConnMRef, process, Conn, _}
        when {Conn, ConnMRef} =:= ConnTuple ->
            channels_manager(NodeConfig, undefined, Channels);
        {'DOWN', ChMRef, process, Ch, _} ->
            Channels1 = Channels -- [{Ch, ChMRef}],
            channels_manager(NodeConfig, ConnTuple, Channels1);
        stop ->
            close_everything(ConnTuple, Channels);
        Unhandled ->
            ct:pal("Channels manager ~p: unhandled message: ~p",
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

open_channel(Config, Node) ->
    Pid = rabbit_ct_broker_helpers:get_node_config(Config, Node,
      channels_manager),
    Pid ! {open_channel, self()},
    receive
        Ch when is_pid(Ch) -> Ch
    end.

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
