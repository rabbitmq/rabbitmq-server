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
%% Copyright (c) 2020-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_stream).

-behaviour(application).

-export([start/2,
         host/0,
         tls_host/0,
         port/0,
         tls_port/0,
         kill_connection/1]).
-export([stop/1]).
-export([emit_connection_info_local/3,
         emit_connection_info_all/4,
         emit_consumer_info_all/5,
         emit_consumer_info_local/4,
         emit_publisher_info_all/5,
         emit_publisher_info_local/4,
         list/1]).

-include_lib("rabbit_common/include/rabbit.hrl").
-include_lib("rabbitmq_stream_common/include/rabbit_stream.hrl").

-include("rabbit_stream_metrics.hrl").

start(_Type, _Args) ->
    FeatureFlagsEnabled = rabbit_ff_registry:list(enabled),
    case maps:is_key(stream_queue, FeatureFlagsEnabled) of
    true -> rabbit_stream_metrics:init(),
      rabbit_global_counters:init([{protocol, stream}], ?PROTOCOL_COUNTERS),
      rabbit_global_counters:init([{protocol, stream},
        {queue_type, ?STREAM_QUEUE_TYPE}]),
      rabbit_stream_sup:start_link();
    false ->
       rabbit_log:warning(
         "Unable to start the stream plugin. The feature flag stream_queue is disabled \n"++
         "You need to enable it and restart the broker",
        []),

      {ok, self()}
    end.



tls_host() ->
    case application:get_env(rabbitmq_stream, advertised_tls_host,
                             undefined)
    of
        undefined ->
            host();
        Host ->
            rabbit_data_coercion:to_binary(Host)
    end.

host() ->
    case application:get_env(rabbitmq_stream, advertised_host, undefined)
    of
        undefined ->
            hostname_from_node();
        Host ->
            rabbit_data_coercion:to_binary(Host)
    end.

hostname_from_node() ->
    case re:split(
             rabbit_data_coercion:to_binary(node()), "@",
             [{return, binary}, {parts, 2}])
    of
        [_, Hostname] ->
            Hostname;
        [_] ->
            {ok, H} = inet:gethostname(),
            rabbit_data_coercion:to_binary(H)
    end.

port() ->
    case application:get_env(rabbitmq_stream, advertised_port, undefined)
    of
        undefined ->
            port_from_listener();
        Port ->
            Port
    end.

port_from_listener() ->
    Listeners = rabbit_networking:node_listeners(node()),
    Port =
        lists:foldl(fun (#listener{port = Port, protocol = stream}, _Acc) ->
                            Port;
                        (_, Acc) ->
                            Acc
                    end,
                    undefined, Listeners),
    Port.

tls_port() ->
    case application:get_env(rabbitmq_stream, advertised_tls_port,
                             undefined)
    of
        undefined ->
            tls_port_from_listener();
        Port ->
            Port
    end.

tls_port_from_listener() ->
    Listeners = rabbit_networking:node_listeners(node()),
    Port =
        lists:foldl(fun (#listener{port = Port, protocol = 'stream/ssl'},
                         _Acc) ->
                            Port;
                        (_, Acc) ->
                            Acc
                    end,
                    undefined, Listeners),
    Port.

stop(_State) ->
    ok.

kill_connection(ConnectionName) ->
    ConnectionNameBin = rabbit_data_coercion:to_binary(ConnectionName),
    lists:foreach(fun(ConnectionPid) ->
                     ConnectionPid ! {infos, self()},
                     receive
                         {ConnectionPid,
                          #{<<"connection_name">> := ConnectionNameBin}} ->
                             exit(ConnectionPid, kill);
                         {ConnectionPid, _ClientProperties} -> ok
                     after 1000 -> ok
                     end
                  end,
                  pg_local:get_members(rabbit_stream_connections)).

emit_connection_info_all(Nodes, Items, Ref, AggregatorPid) ->
    Pids =
        [spawn_link(Node,
                    rabbit_stream,
                    emit_connection_info_local,
                    [Items, Ref, AggregatorPid])
         || Node <- Nodes],
    rabbit_control_misc:await_emitters_termination(Pids),
    ok.

emit_connection_info_local(Items, Ref, AggregatorPid) ->
    rabbit_control_misc:emitting_map_with_exit_handler(AggregatorPid,
                                                       Ref,
                                                       fun(Pid) ->
                                                          rabbit_stream_reader:info(Pid,
                                                                                    Items)
                                                       end,
                                                       list(undefined)).

emit_consumer_info_all(Nodes, VHost, Items, Ref, AggregatorPid) ->
    Pids =
        [spawn_link(Node,
                    rabbit_stream,
                    emit_consumer_info_local,
                    [VHost, Items, Ref, AggregatorPid])
         || Node <- Nodes],
    rabbit_control_misc:await_emitters_termination(Pids),
    ok.

emit_consumer_info_local(VHost, Items, Ref, AggregatorPid) ->
    rabbit_control_misc:emitting_map_with_exit_handler(AggregatorPid,
                                                       Ref,
                                                       fun(Pid) ->
                                                          rabbit_stream_reader:consumers_info(Pid,
                                                                                              Items)
                                                       end,
                                                       list(VHost)).

emit_publisher_info_all(Nodes, VHost, Items, Ref, AggregatorPid) ->
    Pids =
        [spawn_link(Node,
                    rabbit_stream,
                    emit_publisher_info_local,
                    [VHost, Items, Ref, AggregatorPid])
         || Node <- Nodes],
    rabbit_control_misc:await_emitters_termination(Pids),
    ok.

emit_publisher_info_local(VHost, Items, Ref, AggregatorPid) ->
    rabbit_control_misc:emitting_map_with_exit_handler(AggregatorPid,
                                                       Ref,
                                                       fun(Pid) ->
                                                          rabbit_stream_reader:publishers_info(Pid,
                                                                                               Items)
                                                       end,
                                                       list(VHost)).

list(VHost) ->
    [Client
     || {_, ListSup, _, _}
            <- supervisor2:which_children(rabbit_stream_sup),
        {_, RanchEmbeddedSup, supervisor, _}
            <- supervisor2:which_children(ListSup),
        {{ranch_listener_sup, _}, RanchListSup, _, _}
            <- supervisor:which_children(RanchEmbeddedSup),
        {ranch_conns_sup_sup, RanchConnsSup, supervisor, _}
            <- supervisor2:which_children(RanchListSup),
        {_, RanchConnSup, supervisor, _}
            <- supervisor2:which_children(RanchConnsSup),
        {_, StreamClientSup, supervisor, _}
            <- supervisor2:which_children(RanchConnSup),
        {rabbit_stream_reader, Client, _, _}
            <- supervisor:which_children(StreamClientSup),
        rabbit_stream_reader:in_vhost(Client, VHost)].
