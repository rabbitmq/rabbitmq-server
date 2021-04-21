%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_trace).

-export([init/1, enabled/1, tap_in/6, tap_out/5, start/1, stop/1]).

-include_lib("rabbit_common/include/rabbit.hrl").
-include_lib("rabbit_common/include/rabbit_framing.hrl").

-define(TRACE_VHOSTS, trace_vhosts).
-define(XNAME, <<"amq.rabbitmq.trace">>).

%%----------------------------------------------------------------------------

-type state() :: rabbit_types:exchange() | 'none'.

%%----------------------------------------------------------------------------

-spec init(rabbit_types:vhost()) -> state().

init(VHost) ->
    case enabled(VHost) of
        false -> none;
        true  -> {ok, X} = rabbit_exchange:lookup(
                             rabbit_misc:r(VHost, exchange, ?XNAME)),
                 X
    end.

-spec enabled(rabbit_types:vhost()) -> boolean().

enabled(VHost) ->
    {ok, VHosts} = application:get_env(rabbit, ?TRACE_VHOSTS),
    lists:member(VHost, VHosts).

-spec tap_in(rabbit_types:basic_message(), [rabbit_amqqueue:name()],
                   binary(), rabbit_channel:channel_number(),
                   rabbit_types:username(), state()) -> 'ok'.

tap_in(_Msg, _QNames, _ConnName, _ChannelNum, _Username, none) -> ok;
tap_in(Msg = #basic_message{exchange_name = #resource{name         = XName,
                                                      virtual_host = VHost}},
       QNames, ConnName, ChannelNum, Username, TraceX) ->
    trace(TraceX, Msg, <<"publish">>, XName,
          [{<<"vhost">>,         longstr,   VHost},
           {<<"connection">>,    longstr,   ConnName},
           {<<"channel">>,       signedint, ChannelNum},
           {<<"user">>,          longstr,   Username},
           {<<"routed_queues">>, array,
            [{longstr, QName#resource.name} || QName <- QNames]}]).

-spec tap_out(rabbit_amqqueue:qmsg(), binary(),
                    rabbit_channel:channel_number(),
                    rabbit_types:username(), state()) -> 'ok'.

tap_out(_Msg, _ConnName, _ChannelNum, _Username, none) -> ok;
tap_out({#resource{name = QName, virtual_host = VHost},
         _QPid, _QMsgId, Redelivered, Msg},
        ConnName, ChannelNum, Username, TraceX) ->
    RedeliveredNum = case Redelivered of true -> 1; false -> 0 end,
    trace(TraceX, Msg, <<"deliver">>, QName,
          [{<<"redelivered">>, signedint, RedeliveredNum},
           {<<"vhost">>,       longstr,   VHost},
           {<<"connection">>,  longstr,   ConnName},
           {<<"channel">>,     signedint, ChannelNum},
           {<<"user">>,        longstr,   Username}]).

%%----------------------------------------------------------------------------

-spec start(rabbit_types:vhost()) -> 'ok'.

start(VHost) ->
    rabbit_log:info("Enabling tracing for vhost '~s'", [VHost]),
    update_config(fun (VHosts) -> [VHost | VHosts -- [VHost]] end).

-spec stop(rabbit_types:vhost()) -> 'ok'.

stop(VHost) ->
    rabbit_log:info("Disabling tracing for vhost '~s'", [VHost]),
    update_config(fun (VHosts) -> VHosts -- [VHost] end).

update_config(Fun) ->
    {ok, VHosts0} = application:get_env(rabbit, ?TRACE_VHOSTS),
    VHosts = Fun(VHosts0),
    application:set_env(rabbit, ?TRACE_VHOSTS, VHosts),
    rabbit_channel:refresh_config_local(),
    ok.

%%----------------------------------------------------------------------------

trace(#exchange{name = Name}, #basic_message{exchange_name = Name},
      _RKPrefix, _RKSuffix, _Extra) ->
    ok;
trace(X, Msg = #basic_message{content = #content{payload_fragments_rev = PFR}},
      RKPrefix, RKSuffix, Extra) ->
    ok = rabbit_basic:publish(
                X, <<RKPrefix/binary, ".", RKSuffix/binary>>,
                #'P_basic'{headers = msg_to_table(Msg) ++ Extra}, PFR),
    ok.

msg_to_table(#basic_message{exchange_name = #resource{name = XName},
                            routing_keys  = RoutingKeys,
                            content       = Content}) ->
    #content{properties = Props} = rabbit_binary_parser:ensure_content_decoded(Content),
    #'P_basic'{
        content_type = CT,
        content_encoding = CE,
        headers = Headers,
        delivery_mode = DMode,
        priority = Pr,
        correlation_id = CorrId,
        reply_to = ReplyTo,
        expiration = Exp,
        message_id = MsgId,
        timestamp = Ts,
        type = Type,
        user_id = UserId,
        app_id = AppId,
        cluster_id = ClusterId
    } = Props,

    PropsTable0  = [],
    PropsTable1  = append_property(content_type, CT, PropsTable0),
    PropsTable2  = append_property(content_encoding, CE, PropsTable1),
    PropsTable3  = append_property(headers, Headers, PropsTable2),
    PropsTable4  = append_property(delivery_mode, DMode, PropsTable3),
    PropsTable5  = append_property(priority, Pr, PropsTable4),
    PropsTable6  = append_property(correlation_id, CorrId, PropsTable5),
    PropsTable7  = append_property(reply_to, ReplyTo, PropsTable6),
    PropsTable8  = append_property(expiration, Exp, PropsTable7),
    PropsTable9  = append_property(message_id, MsgId, PropsTable8),
    PropsTable10 = append_property(timestamp, Ts, PropsTable9),
    PropsTable11 = append_property(type, Type, PropsTable10),
    PropsTable12 = append_property(user_id, UserId, PropsTable11),
    PropsTable13 = append_property(app_id, AppId, PropsTable12),
    PropsTable14 = append_property(cluster_id, ClusterId, PropsTable13),

    [
        {<<"exchange_name">>, longstr, XName},
        {<<"routing_keys">>,  array,   [{longstr, K} || K <- RoutingKeys]},
        {<<"properties">>,    table,   PropsTable14},
        {<<"node">>,          longstr, rabbit_data_coercion:to_binary(node())}
    ].

append_property(_Key, undefined, PropsTable) ->
    PropsTable;
append_property(Key = headers, Val, PropsTable) ->
    [{rabbit_data_coercion:to_binary(Key), table, Val} | PropsTable];
append_property(timestamp, Val, PropsTable) ->
    [
        %% TODO: should timestamp_in_ms added?
        {<<"timestamp">>, long, Val} | PropsTable
    ];
append_property(Key, Val, PropsTable) when is_integer(Val) ->
    [{rabbit_data_coercion:to_binary(Key), signedint, Val} | PropsTable];
append_property(Key, Val, PropsTable) ->
    [{rabbit_data_coercion:to_binary(Key), longstr, Val} | PropsTable].
