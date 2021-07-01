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
    case lists:member(VHost, vhosts_with_tracing_enabled()) of
        true  ->
            _ = rabbit_log:info("Tracing is already enabled for vhost '~s'", [VHost]),
            ok;
        false ->
            _ = rabbit_log:info("Enabling tracing for vhost '~s'", [VHost]),
            update_config(fun (VHosts) ->
                            lists:usort([VHost | VHosts])
                          end)
    end.

-spec stop(rabbit_types:vhost()) -> 'ok'.

stop(VHost) ->
    case lists:member(VHost, vhosts_with_tracing_enabled()) of
        true  ->
            _ = rabbit_log:info("Disabling tracing for vhost '~s'", [VHost]),
            update_config(fun (VHosts) -> VHosts -- [VHost] end);
        false ->
            _ = rabbit_log:info("Tracing is already disabled for vhost '~s'", [VHost]),
            ok
    end.

update_config(Fun) ->
    VHosts0 = vhosts_with_tracing_enabled(),
    VHosts = Fun(VHosts0),
    application:set_env(rabbit, ?TRACE_VHOSTS, VHosts),
    _ = rabbit_log:debug("Will now refresh channel state after virtual host tracing changes"),

    {Time, _} = timer:tc(fun rabbit_channel:refresh_config_local/0),
    _ = rabbit_log:debug("Refreshed channel state in ~fs", [Time/1000000]),

    ok.

vhosts_with_tracing_enabled() ->
    application:get_env(rabbit, ?TRACE_VHOSTS, []).

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
    #content{properties = Props} =
        rabbit_binary_parser:ensure_content_decoded(Content),
    {PropsTable, _Ix} =
        lists:foldl(fun (K, {L, Ix}) ->
                            V = element(Ix, Props),
                            NewL = case V of
                                       undefined -> L;
                                       _         -> [{a2b(K), type(V), V} | L]
                                   end,
                            {NewL, Ix + 1}
                    end, {[], 2}, record_info(fields, 'P_basic')),
    [{<<"exchange_name">>, longstr, XName},
     {<<"routing_keys">>,  array,   [{longstr, K} || K <- RoutingKeys]},
     {<<"properties">>,    table,   PropsTable},
     {<<"node">>,          longstr, a2b(node())}].

a2b(A) -> list_to_binary(atom_to_list(A)).

type(V) when is_list(V)    -> table;
type(V) when is_integer(V) -> signedint;
type(_V)                   -> longstr.
