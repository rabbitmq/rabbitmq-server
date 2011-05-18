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
%% Copyright (c) 2007-2011 VMware, Inc.  All rights reserved.
%%

-module(rabbit_trace).

-export([init/1, tap_trace_in/2, tap_trace_out/2]).

-include("rabbit.hrl").
-include("rabbit_framing.hrl").

-record(trace_state, {trace_exchange}).

%%----------------------------------------------------------------------------

-ifdef(use_specs).

-type(state() :: #trace_state{trace_exchange :: rabbit_exchange:name()}).

-spec(init/1 :: (rabbit_types:vhost()) -> state()).
-spec(tap_trace_in/2 :: (rabbit_types:basic_message(), state()) -> 'ok').
-spec(tap_trace_out/2 :: (rabbit_amqqueue:qmsg(), state()) -> 'ok').

-endif.

%%----------------------------------------------------------------------------

init(VHost) ->
    #trace_state{trace_exchange = trace_exchange(VHost)}.

tap_trace_in(Msg, #trace_state{trace_exchange = TraceX}) ->
    maybe_trace(Msg, TraceX, <<"publish">>, xname(Msg), []).

tap_trace_out({#resource{name = QName}, _QPid, _QMsgId, Redelivered, Msg},
              #trace_state{trace_exchange = TraceX}) ->
    RedeliveredNum = case Redelivered of true -> 1; false -> 0 end,
    maybe_trace(Msg, TraceX, <<"deliver">>, QName,
                [{<<"redelivered">>, signedint, RedeliveredNum}]).

xname(#basic_message{exchange_name = #resource{name         = XName}}) -> XName.
vhost(#basic_message{exchange_name = #resource{virtual_host = VHost}}) -> VHost.

maybe_trace(Msg, TraceX, RKPrefix, RKSuffix, Extra) ->
    XName = xname(Msg),
    case TraceX of
        none   -> ok;
        XName  -> ok;
        _      -> case catch trace(TraceX, Msg, RKPrefix, RKSuffix, Extra) of
                      {'EXIT', R} -> rabbit_log:info("Trace died: ~p~n", [R]);
                      ok          -> ok
                  end
    end.

trace_exchange(VHost) ->
    case application:get_env(rabbit, trace_exchanges) of
        undefined  -> none;
        {ok, Xs}   -> proplists:get_value(VHost, Xs, none)
    end.

trace(TraceX, Msg0, RKPrefix, RKSuffix, Extra) ->
    Msg = ensure_content_decoded(Msg0),
    rabbit_basic:publish(rabbit_misc:r(vhost(Msg), exchange, TraceX),
                         <<RKPrefix/binary, ".", RKSuffix/binary>>,
                         #'P_basic'{headers = msg_to_table(Msg) ++ Extra},
                         payload(Msg)),
    ok.

msg_to_table(#basic_message{exchange_name = #resource{name = XName},
                            routing_keys  = RoutingKeys,
                            content       = #content{properties = Props}}) ->
    {PropsTable, _Ix} =
        lists:foldl(
          fun (K, {L, Ix}) ->
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

payload(#basic_message{content = #content{payload_fragments_rev = PFR}}) ->
    list_to_binary(lists:reverse(PFR)).

ensure_content_decoded(Msg = #basic_message{content = Content}) ->
    Msg#basic_message{content = rabbit_binary_parser:ensure_content_decoded(
                                  Content)}.

a2b(A) ->
    list_to_binary(atom_to_list(A)).

type(V) when is_list(V)    -> table;
type(V) when is_integer(V) -> signedint;
type(_V)                   -> longstr.
