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
    prepare_connections/1,
    terminate_connections/1,
    open_channel/2, close_channel/1
  ]).

%% -------------------------------------------------------------------
%% Client setup/teardown steps.
%% -------------------------------------------------------------------

prepare_connections(Config) ->
    NodeConfigs = rabbit_ct_broker_helpers:get_node_configs(Config),
    NodeConfigs1 = [prepare_connection(NC) || NC <- NodeConfigs],
    rabbit_ct_helpers:set_config(Config, {rmq_nodes, NodeConfigs1}).

prepare_connection(NodeConfig) ->
    Port = ?config(tcp_port_amqp, NodeConfig),
    {ok, Conn} = amqp_connection:start(#amqp_params_network{port = Port}),
    rabbit_ct_helpers:set_config(NodeConfig, {connection, Conn}).

terminate_connections(Config) ->
    NodeConfigs = rabbit_ct_broker_helpers:get_node_configs(Config),
    NodeConfigs1 = [terminate_connection(NC) || NC <- NodeConfigs],
    rabbit_ct_helpers:set_config(Config, {rmq_nodes, NodeConfigs1}).

terminate_connection(NodeConfig) ->
    case rabbit_ct_helpers:get_config(NodeConfig, connection) of
        undefined ->
            NodeConfig;
        Conn ->
            case is_process_alive(Conn) of
                true  -> amqp_connection:close(Conn);
                false -> ok
            end,
            proplists:delete(connection, NodeConfig)
    end.

%% -------------------------------------------------------------------
%% Other helpers.
%% -------------------------------------------------------------------

open_channel(Config, I) ->
    Conn = rabbit_ct_broker_helpers:get_node_config(Config, I, connection),
    amqp_connection:open_channel(Conn).

close_channel(Ch) ->
    amqp_channel:close(Ch).
