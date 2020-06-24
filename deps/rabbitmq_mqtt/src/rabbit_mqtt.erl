%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License
%% at https://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and
%% limitations under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2007-2020 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_mqtt).

-behaviour(application).
-export([start/2, stop/1]).
-export([connection_info_local/1,
         emit_connection_info_local/3,
         emit_connection_info_all/4]).

start(normal, []) ->
    {ok, Listeners} = application:get_env(tcp_listeners),
    {ok, SslListeners} = application:get_env(ssl_listeners),
    ok = mqtt_node:start(),
    Result = rabbit_mqtt_sup:start_link({Listeners, SslListeners}, []),
    EMPid = case rabbit_event:start_link() of
              {ok, Pid}                       -> Pid;
              {error, {already_started, Pid}} -> Pid
            end,
    gen_event:add_handler(EMPid, rabbit_mqtt_vhost_event_handler, []),
    Result.

stop(_) ->
    rabbit_mqtt_sup:stop_listeners().

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
