%% The contents of this file are subject to the Mozilla Public License
%% Version 2.0 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License
%% at https://www.mozilla.org/en-US/MPL/2.0/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and
%% limitations under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is Pivotal Software, Inc.
%% Copyright (c) 2020 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_stream).
-behaviour(application).

-export([start/2, host/0, port/0, kill_connection/1]).
-export([stop/1]).
-export([emit_connection_info_local/3,
    emit_connection_info_all/4,
    list/0]).

-include_lib("rabbit_common/include/rabbit.hrl").

start(_Type, _Args) ->
    rabbit_stream_sup:start_link().

host() ->
    case application:get_env(rabbitmq_stream, advertised_host, undefined) of
        undefined ->
            {ok, Host} = inet:gethostname(),
            list_to_binary(Host);
        Host ->
            rabbit_data_coercion:to_binary(Host)
    end.

port() ->
    case application:get_env(rabbitmq_stream, advertised_port, undefined) of
        undefined ->
            port_from_listener();
        Port ->
            Port
    end.

port_from_listener() ->
    Listeners = rabbit_networking:node_listeners(node()),
    Port = lists:foldl(fun(#listener{port = Port, protocol = stream}, _Acc) ->
        Port;
        (_, Acc) ->
            Acc
                       end, undefined, Listeners),
    Port.

stop(_State) ->
    ok.

kill_connection(ConnectionName) ->
    ConnectionNameBin = rabbit_data_coercion:to_binary(ConnectionName),
    lists:foreach(fun(ConnectionPid) ->
        ConnectionPid ! {infos, self()},
        receive
            {ConnectionPid, #{<<"name">> := ConnectionNameBin}} ->
                exit(ConnectionPid, kill);
            {ConnectionPid, _ClientProperties} ->
                ok
        after 1000 ->
            ok
        end
                  end, pg_local:get_members(rabbit_stream_connections)).

emit_connection_info_all(Nodes, Items, Ref, AggregatorPid) ->
    Pids = [spawn_link(Node, rabbit_stream, emit_connection_info_local,
        [Items, Ref, AggregatorPid])
        || Node <- Nodes],
    rabbit_control_misc:await_emitters_termination(Pids),
    ok.

emit_connection_info_local(Items, Ref, AggregatorPid) ->
    rabbit_control_misc:emitting_map_with_exit_handler(
        AggregatorPid, Ref, fun(Pid) ->
            rabbit_stream_reader:info(Pid, Items)
                            end,
        list()).

list() ->
    [Client
        || {_, ListSupPid, _, _} <- supervisor2:which_children(rabbit_stream_sup),
        {_, RanchSup, supervisor, _} <- supervisor2:which_children(ListSupPid),
        {ranch_conns_sup, ConnSup, _, _} <- supervisor:which_children(RanchSup),
        {_, CliSup, _, _} <- supervisor:which_children(ConnSup),
        {rabbit_stream_reader, Client, _, _} <- supervisor:which_children(CliSup)].