%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_mqtt).

-behaviour(application).
-export([start/2, stop/1]).
-export([connection_info_local/1,
         emit_connection_info_local/3,
         emit_connection_info_all/4,
         close_all_client_connections/1]).

start(normal, []) ->
    {ok, Listeners} = application:get_env(tcp_listeners),
    {ok, SslListeners} = application:get_env(ssl_listeners),
    ok = mqtt_node:start(),
    Result = rabbit_mqtt_sup:start_link({Listeners, SslListeners}, []),
    EMPid = case rabbit_event:start_link() of
              {ok, Pid}                       -> Pid;
              {error, {already_started, Pid}} -> Pid
            end,
    gen_event:add_handler(EMPid, rabbit_mqtt_internal_event_handler, []),
    Result.

stop(_) ->
    rabbit_mqtt_sup:stop_listeners().

-spec close_all_client_connections(string() | binary()) -> {'ok', non_neg_integer()}.
close_all_client_connections(Reason) ->
     Connections = rabbit_mqtt_collector:list(),
    [rabbit_mqtt_reader:close_connection(Pid, Reason) || {_, Pid} <- Connections],
    {ok, length(Connections)}.

emit_connection_info_all(Nodes, Items, Ref, AggregatorPid) ->
    Pids = [spawn_link(Node, rabbit_mqtt, emit_connection_info_local,
                       [Items, Ref, AggregatorPid])
            || Node <- Nodes],
    rabbit_control_misc:await_emitters_termination(Pids),
    ok.

emit_connection_info_local(Items, Ref, AggregatorPid) ->
    rabbit_control_misc:emitting_map_with_exit_handler(
        AggregatorPid, Ref, fun({_, Pid}) ->
            rabbit_mqtt_reader:info(Pid, Items)
        end,
        rabbit_mqtt_collector:list()).

connection_info_local(Items) ->
    Connections = rabbit_mqtt_collector:list(),
    [rabbit_mqtt_reader:info(Pid, Items)
     || {_, Pid} <- Connections].
