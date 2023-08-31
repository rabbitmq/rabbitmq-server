%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2023 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_mqtt).

-behaviour(application).

-include("rabbit_mqtt.hrl").
-include("rabbit_mqtt_packet.hrl").
-include_lib("rabbit/include/rabbit_global_counters.hrl").
-include_lib("stdlib/include/assert.hrl").

-export([start/2, stop/1]).
-export([emit_connection_info_all/4,
         emit_connection_info_local/3,
         close_local_client_connections/1,
         %% Exported for tests, but could also be used for debugging.
         local_connection_pids/0]).

start(normal, []) ->
    init_global_counters(),
    persist_static_configuration(),
    {ok, Listeners} = application:get_env(tcp_listeners),
    {ok, SslListeners} = application:get_env(ssl_listeners),
    case rabbit_mqtt_ff:track_client_id_in_ra() of
        true ->
            ok = mqtt_node:start();
        false ->
            ok
    end,
    Result = rabbit_mqtt_sup:start_link({Listeners, SslListeners}, []),
    EMPid = case rabbit_event:start_link() of
                {ok, Pid}                       -> Pid;
                {error, {already_started, Pid}} -> Pid
            end,
    gen_event:add_handler(EMPid, rabbit_mqtt_internal_event_handler, []),
    Result.

stop(_) ->
    rabbit_mqtt_sup:stop_listeners().

-spec emit_connection_info_all([node()], rabbit_types:info_keys(), reference(), pid()) -> term().
emit_connection_info_all(Nodes, Items, Ref, AggregatorPid) ->
    case rabbit_mqtt_ff:track_client_id_in_ra() of
        true ->
            %% Ra tracks connections cluster-wide.
            AllPids = rabbit_mqtt_collector:list_pids(),
            emit_connection_info(Items, Ref, AggregatorPid, AllPids),
            %% Our node already emitted infos for all connections. Therefore, for the
            %% remaining nodes, we send back 'finished' so that the CLI does not time out.
            [AggregatorPid ! {Ref, finished} || _ <- lists:seq(1, length(Nodes) - 1)];
        false ->
            Pids = [spawn_link(Node, ?MODULE, emit_connection_info_local,
                               [Items, Ref, AggregatorPid])
                    || Node <- Nodes],
            rabbit_control_misc:await_emitters_termination(Pids)
    end.

-spec emit_connection_info_local(rabbit_types:info_keys(), reference(), pid()) -> ok.
emit_connection_info_local(Items, Ref, AggregatorPid) ->
    LocalPids = local_connection_pids(),
    emit_connection_info(Items, Ref, AggregatorPid, LocalPids).

emit_connection_info(Items, Ref, AggregatorPid, Pids) ->
    rabbit_control_misc:emitting_map_with_exit_handler(
      AggregatorPid, Ref,
      fun(Pid) ->
              rabbit_mqtt_reader:info(Pid, Items)
      end, Pids).

-spec close_local_client_connections(atom()) -> {'ok', non_neg_integer()}.
close_local_client_connections(Reason) ->
    Pids = local_connection_pids(),
    lists:foreach(fun(Pid) ->
                          rabbit_mqtt_reader:close_connection(Pid, Reason)
                  end, Pids),
    {ok, length(Pids)}.

-spec local_connection_pids() -> [pid()].
local_connection_pids() ->
    case rabbit_mqtt_ff:track_client_id_in_ra() of
        true ->
            AllPids = rabbit_mqtt_collector:list_pids(),
            lists:filter(fun(Pid) -> node(Pid) =:= node() end, AllPids);
        false ->
            PgScope = persistent_term:get(?PG_SCOPE),
            lists:flatmap(fun(Group) ->
                                  pg:get_local_members(PgScope, Group)
                          end, pg:which_groups(PgScope))
    end.

init_global_counters() ->
    lists:foreach(fun init_global_counters/1, [?MQTT_PROTO_V3,
                                               ?MQTT_PROTO_V4,
                                               ?MQTT_PROTO_V5]),
    rabbit_global_counters:init([{queue_type, ?QUEUE_TYPE_QOS_0}, {dead_letter_strategy, disabled}],
                                [?MESSAGES_DEAD_LETTERED_MAXLEN_COUNTER]).

init_global_counters(ProtoVer) ->
    Proto = {protocol, ProtoVer},
    rabbit_global_counters:init([Proto]),
    rabbit_global_counters:init([Proto, {queue_type, rabbit_classic_queue}]),
    rabbit_global_counters:init([Proto, {queue_type, rabbit_quorum_queue}]),
    rabbit_global_counters:init([Proto, {queue_type, ?QUEUE_TYPE_QOS_0}]).

persist_static_configuration() ->
    rabbit_mqtt_util:init_sparkplug(),

    {ok, Exchange} = application:get_env(?APP_NAME, exchange),
    ?assert(is_binary(Exchange)),
    ok = persistent_term:put(?PERSISTENT_TERM_EXCHANGE, Exchange),

    {ok, MailboxSoftLimit} = application:get_env(?APP_NAME, mailbox_soft_limit),
    ?assert(is_integer(MailboxSoftLimit)),
    ok = persistent_term:put(?PERSISTENT_TERM_MAILBOX_SOFT_LIMIT, MailboxSoftLimit),

    {ok, TopicAliasMax} = application:get_env(?APP_NAME, topic_alias_maximum),
    ?assert(is_integer(TopicAliasMax) andalso
            TopicAliasMax >= 0 andalso
            TopicAliasMax =< ?TWO_BYTE_INTEGER_MAX),
    ok = persistent_term:put(?PERSISTENT_TERM_TOPIC_ALIAS_MAXIMUM, TopicAliasMax),

    {ok, MaxSizeUnauth} = application:get_env(?APP_NAME, max_packet_size_unauthenticated),
    assert_valid_max_packet_size(MaxSizeUnauth),
    ok = persistent_term:put(?PERSISTENT_TERM_MAX_PACKET_SIZE_UNAUTHENTICATED, MaxSizeUnauth),

    {ok, MaxSizeAuth} = application:get_env(?APP_NAME, max_packet_size_authenticated),
    assert_valid_max_packet_size(MaxSizeAuth),
    ok = persistent_term:put(?PERSISTENT_TERM_MAX_PACKET_SIZE_AUTHENTICATED, MaxSizeAuth).

assert_valid_max_packet_size(Val) ->
    ?assert(is_integer(Val) andalso
            Val > 0 andalso
            Val =< ?MAX_PACKET_SIZE).
