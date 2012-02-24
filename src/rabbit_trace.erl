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
%% The Initial Developer of the Original Code is VMware, Inc.
%% Copyright (c) 2007-2012 VMware, Inc.  All rights reserved.
%%

-module(rabbit_trace).

-export([init/3, tracing/1, tap_trace_in/2, tap_trace_out/2,
         tap_trace_method_in/3, tap_trace_method_out/3, start/1, stop/1]).

-include("rabbit.hrl").
-include("rabbit_framing.hrl").

-define(TRACE_VHOSTS, trace_vhosts).
-define(XNAME, <<"amq.rabbitmq.trace">>).

%%----------------------------------------------------------------------------

-ifdef(use_specs).

-type(state() :: rabbit_types:exchange() | 'none').

%%-spec(init/1 :: (rabbit_types:vhost()) -> state()).
-spec(tracing/1 :: (rabbit_types:vhost()) -> boolean()).
-spec(tap_trace_in/2 :: (rabbit_types:basic_message(), state()) -> 'ok').
-spec(tap_trace_out/2 :: (rabbit_amqqueue:qmsg(), state()) -> 'ok').

-spec(start/1 :: (rabbit_types:vhost()) -> 'ok').
-spec(stop/1 :: (rabbit_types:vhost()) -> 'ok').

-endif.

%%----------------------------------------------------------------------------

init(VHost, #user{tags = Tags}, ClientProps) ->
    Suppress = rabbit_misc:table_lookup(ClientProps, <<"suppress-tracing">>)
        =:= {bool, true},
    case tracing(VHost) andalso
        not (Suppress andalso lists:member(administrator, Tags)) of
        false -> none;
        true  -> {ok, X} = rabbit_exchange:lookup(
                             rabbit_misc:r(VHost, exchange, ?XNAME)),
                 X
    end.

tracing(VHost) ->
    {ok, VHosts} = application:get_env(rabbit, ?TRACE_VHOSTS),
    lists:member(VHost, VHosts).

tap_trace_in(Msg = #basic_message{exchange_name = #resource{name = XName}},
             TraceX) ->
    maybe_trace(TraceX, Msg, <<"publish.", XName/binary>>, []).

tap_trace_out({#resource{name = QName}, _QPid, _QMsgId, Redelivered, Msg},
              TraceX) ->
    RedeliveredNum = case Redelivered of true -> 1; false -> 0 end,
    maybe_trace(TraceX, Msg, <<"deliver.", QName/binary>>,
                [{<<"redelivered">>, signedint, RedeliveredNum}]).

tap_trace_method_in(Ch, Method, TraceX) ->
    tap_trace_method(Ch, Method, TraceX, <<"method_in">>).

tap_trace_method_out(Ch, Method, TraceX) ->
    tap_trace_method(Ch, Method, TraceX, <<"method_out">>).

tap_trace_method(Ch, Method, TraceX, Prefix) ->
    MethodName = a2b(element(1, Method)),
    maybe_trace(TraceX, Method,
                <<Prefix/binary, ".", MethodName/binary,".", Ch/binary>>,
                []).

%%----------------------------------------------------------------------------

start(VHost) ->
    rabbit_log:info("Enabling tracing for vhost '~s'~n", [VHost]),
    update_config(fun (VHosts) -> [VHost | VHosts -- [VHost]] end).

stop(VHost) ->
    rabbit_log:info("Disabling tracing for vhost '~s'~n", [VHost]),
    update_config(fun (VHosts) -> VHosts -- [VHost] end).

update_config(Fun) ->
    {ok, VHosts0} = application:get_env(rabbit, ?TRACE_VHOSTS),
    VHosts = Fun(VHosts0),
    application:set_env(rabbit, ?TRACE_VHOSTS, VHosts),
    rabbit_channel:refresh_config_local(),
    ok.

%%----------------------------------------------------------------------------

maybe_trace(none, _Msg, _RoutingKey, _Extra) ->
    ok;
maybe_trace(#exchange{name = Name}, #basic_message{exchange_name = Name},
            _RoutingKey, _Extra) ->
    ok;
maybe_trace(X, Event, RoutingKey, Extra) ->
    {Headers, Body} = event_to_table(Event),
    {ok, _, _} = rabbit_basic:publish(
                   X, RoutingKey, #'P_basic'{headers = Headers ++ Extra}, Body),
    ok.

event_to_table(#basic_message{exchange_name = #resource{name = XName},
                              routing_keys  = RoutingKeys,
                              content       = Content}) ->
    #content{properties            = Props,
             payload_fragments_rev = PFR} =
        rabbit_binary_parser:ensure_content_decoded(Content),
    PropsTable = record_to_table(record_info(fields, 'P_basic'), Props),
    {[{<<"exchange_name">>, longstr, XName},
      {<<"routing_keys">>,  array,   [{longstr, K} || K <- RoutingKeys]},
      {<<"properties">>,    table,   PropsTable},
      {<<"node">>,          longstr, a2b(node())}], PFR};

event_to_table(Method) ->
    Protocol = rabbit_framing_amqp_0_9_1, %% TODO duh
    MethodName = element(1, Method),
    Arguments = record_to_table(Protocol:method_fieldnames(MethodName), Method),
    {[{<<"method">>,     longstr, a2b(MethodName)},
      {<<"parameters">>, table,   Arguments}], []}.

record_to_table(FieldNames, Record) ->
    {Table, _Ix} =
        lists:foldl(fun (K, {L, Ix}) ->
                            V = element(Ix, Record),
                            NewL = case V of
                                       undefined -> L;
                                       _         -> [{a2b(K), type(V), V} | L]
                                   end,
                            {NewL, Ix + 1}
                    end, {[], 2}, FieldNames),
    lists:reverse(Table).

a2b(A) -> list_to_binary(atom_to_list(A)).

type(V) when is_list(V)    -> table;
type(V) when is_integer(V) -> signedint;
type(_V)                   -> longstr.
