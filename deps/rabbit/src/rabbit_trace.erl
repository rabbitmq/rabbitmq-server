%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_trace).

-export([init/1, enabled/1, tap_in/5, tap_in/6,
         tap_out/4, tap_out/5, start/1, stop/1]).

-include_lib("rabbit_common/include/rabbit.hrl").
-include_lib("rabbit_common/include/rabbit_framing.hrl").

-define(TRACE_VHOSTS, trace_vhosts).
-define(XNAME, <<"amq.rabbitmq.trace">>).
%% "The channel number is 0 for all frames which are global to the connection" [AMQP 0-9-1 spec]
-define(CONNECTION_GLOBAL_CHANNEL_NUM, 0).

%%----------------------------------------------------------------------------

-opaque state() :: rabbit_types:exchange() | 'none'.
-export_type([state/0]).

%%----------------------------------------------------------------------------

-spec init(rabbit_types:vhost()) -> state().
init(VHost)
  when is_binary(VHost) ->
    case enabled(VHost) of
        false ->
            none;
        true ->
            {ok, X} = rabbit_exchange:lookup(rabbit_misc:r(VHost, exchange, ?XNAME)),
            X
    end.

-spec enabled(rabbit_types:vhost() | state()) -> boolean().
enabled(none) ->
    false;
enabled(#exchange{}) ->
    true;
enabled(VHost)
  when is_binary(VHost) ->
    lists:member(VHost, vhosts_with_tracing_enabled()).

-spec tap_in(mc:state(), rabbit_exchange:route_return(),
             binary(), rabbit_types:username(), state()) -> 'ok'.
tap_in(Msg, QNames, ConnName, Username, State) ->
    tap_in(Msg, QNames, ConnName, ?CONNECTION_GLOBAL_CHANNEL_NUM, Username, State).

-spec tap_in(mc:state(), rabbit_exchange:route_return(),
             binary(), rabbit_channel:channel_number(),
             rabbit_types:username(), state()) -> 'ok'.
tap_in(_Msg, _QNames, _ConnName, _ChannelNum, _Username, none) ->
    ok;
tap_in(Msg, QNames, ConnName, ChannelNum, Username, TraceX) ->
    XName = mc:exchange(Msg),
    #exchange{name = #resource{virtual_host = VHost}} = TraceX,
    RoutedQs = lists:map(fun(#resource{kind = queue, name = Name}) ->
                                 {longstr, Name};
                            ({#resource{kind = queue, name = Name}, _}) ->
                                 {longstr, Name};
                            ({virtual_reply_queue, Name}) ->
                                 {longstr, Name}
                         end, QNames),
    trace(TraceX, Msg, <<"publish">>, XName,
          [{<<"vhost">>,         longstr,   VHost},
           {<<"connection">>,    longstr,   ConnName},
           {<<"channel">>,       signedint, ChannelNum},
           {<<"user">>,          longstr,   Username},
           {<<"routed_queues">>, array,     RoutedQs}]).

-spec tap_out(rabbit_amqqueue:qmsg(), binary(),
              rabbit_types:username(), state()) -> 'ok'.
tap_out(Msg, ConnName, Username, State) ->
    tap_out(Msg, ConnName, ?CONNECTION_GLOBAL_CHANNEL_NUM, Username, State).

-spec tap_out(rabbit_amqqueue:qmsg(), binary(),
              rabbit_channel:channel_number(),
              rabbit_types:username(), state()) -> 'ok'.
tap_out(_Msg, _ConnName, _ChannelNum, _Username, none) ->
    ok;
tap_out({#resource{name = QName, virtual_host = VHost},
         _QPid, _QMsgId, Redelivered, Msg},
        ConnName, ChannelNum, Username, TraceX) ->
    RedeliveredNum = case Redelivered of
                         true -> 1;
                         false -> 0
                     end,
    trace(TraceX, Msg, <<"deliver">>, QName,
          [{<<"redelivered">>, signedint, RedeliveredNum},
           {<<"vhost">>,       longstr,   VHost},
           {<<"connection">>,  longstr,   ConnName},
           {<<"channel">>,     signedint, ChannelNum},
           {<<"user">>,        longstr,   Username}]).

%%----------------------------------------------------------------------------

-spec start(rabbit_types:vhost()) -> 'ok'.
start(VHost)
  when is_binary(VHost) ->
    case enabled(VHost) of
        true  ->
            rabbit_log:info("Tracing is already enabled for vhost '~ts'", [VHost]),
            ok;
        false ->
            rabbit_log:info("Enabling tracing for vhost '~ts'", [VHost]),
            update_config(fun(VHosts) -> lists:usort([VHost | VHosts]) end)
    end.

-spec stop(rabbit_types:vhost()) -> 'ok'.
stop(VHost)
  when is_binary(VHost) ->
    case enabled(VHost) of
        true  ->
            rabbit_log:info("Disabling tracing for vhost '~ts'", [VHost]),
            update_config(fun(VHosts) -> VHosts -- [VHost] end);
        false ->
            rabbit_log:info("Tracing is already disabled for vhost '~ts'", [VHost]),
            ok
    end.

update_config(Fun) ->
    VHosts0 = vhosts_with_tracing_enabled(),
    VHosts = Fun(VHosts0),
    application:set_env(rabbit, ?TRACE_VHOSTS, VHosts),
    Sessions = rabbit_amqp_session:list_local(),
    NonAmqpPids = rabbit_networking:local_non_amqp_connections(),
    rabbit_log:debug("Refreshing state of channels, ~b sessions and ~b non "
                     "AMQP 0.9.1 connections after virtual host tracing changes...",
                     [length(Sessions), length(NonAmqpPids)]),
    Pids = Sessions ++ NonAmqpPids,
    lists:foreach(fun(Pid) -> gen_server:cast(Pid, refresh_config) end, Pids),
    {Time, ok} = timer:tc(fun rabbit_channel:refresh_config_local/0),
    rabbit_log:debug("Refreshed channel states in ~fs", [Time / 1_000_000]),
    ok.

vhosts_with_tracing_enabled() ->
    {ok, Vhosts} = application:get_env(rabbit, ?TRACE_VHOSTS),
    Vhosts.

%%----------------------------------------------------------------------------

trace(X, Msg0, RKPrefix, RKSuffix, Extra) ->
    XName = mc:exchange(Msg0),
    case X of
        #exchange{name = #resource{name = XName}} ->
            ok;
        #exchange{name = SourceXName} ->
            RoutingKeys = mc:routing_keys(Msg0),
            %% for now convert into amqp legacy
            Msg = mc:prepare(read, mc:convert(mc_amqpl, Msg0)),
            #content{properties = Props} = Content0 = mc:protocol_state(Msg),

            Key = <<RKPrefix/binary, ".", RKSuffix/binary>>,
            Content = Content0#content{properties =
                                       #'P_basic'{headers = msg_to_table(XName, RoutingKeys, Props)
                                                  ++ Extra},
                                       properties_bin = none},
            TargetXName = SourceXName#resource{name = ?XNAME},
            {ok, TraceMsg} = mc_amqpl:message(TargetXName, Key, Content),
            ok = rabbit_queue_type:publish_at_most_once(X, TraceMsg)
    end.

msg_to_table(XName, RoutingKeys, Props) ->
    {PropsTable, _Ix} =
        lists:foldl(fun(K, {L, Ix}) ->
                            V = element(Ix, Props),
                            NewL = case V of
                                       undefined -> L;
                                       _ -> [{atom_to_binary(K), type(V), V} | L]
                                   end,
                            {NewL, Ix + 1}
                    end, {[], 2}, record_info(fields, 'P_basic')),
    [{<<"exchange_name">>, longstr, XName},
     {<<"routing_keys">>,  array,   [{longstr, K} || K <- RoutingKeys]},
     {<<"properties">>,    table,   PropsTable},
     {<<"node">>,          longstr, atom_to_binary(node())}].

type(V) when is_list(V)    -> table;
type(V) when is_integer(V) -> signedint;
type(_V)                   -> longstr.
