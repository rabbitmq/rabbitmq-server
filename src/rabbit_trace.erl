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

%%----------------------------------------------------------------------------

-ifdef(use_specs).

-type(state() :: rabbit_exchange:name() | 'none').

-spec(init/1 :: (rabbit_types:vhost()) -> state()).
-spec(tap_trace_in/2 :: (rabbit_types:basic_message(), state()) -> 'ok').
-spec(tap_trace_out/2 :: (rabbit_amqqueue:qmsg(), state()) -> 'ok').

-endif.

%%----------------------------------------------------------------------------

init(VHost) ->
    case application:get_env(rabbit, trace_exchanges) of
        undefined  -> none;
        {ok, XNs}  -> case proplists:get_value(VHost, XNs, none) of
                          none -> none;
                          Name -> rabbit_misc:r(VHost, exchange, Name)
                      end
    end.

tap_trace_in(Msg, TraceXN) ->
    maybe_trace(Msg, TraceXN, <<"publish">>, xname(Msg), []).

tap_trace_out({#resource{name = QName}, _QPid, _QMsgId, Redelivered, Msg},
              TraceXN) ->
    RedeliveredNum = case Redelivered of true -> 1; false -> 0 end,
    maybe_trace(Msg, TraceXN, <<"deliver">>, QName,
                [{<<"redelivered">>, signedint, RedeliveredNum}]).

maybe_trace(_Msg, none, _RKPrefix, _RKSuffix, _Extra) ->
    ok;
maybe_trace(Msg, XN, RKPrefix, RKSuffix, Extra) ->
    case xname(Msg) of
        XN -> ok;
        _  -> case rabbit_basic:publish(
                     XN,
                     <<RKPrefix/binary, ".", RKSuffix/binary>>,
                     #'P_basic'{headers = msg_to_table(Msg) ++ Extra},
                     payload(Msg)) of
                  {ok, _, _}         -> ok;
                  {error, not_found} -> rabbit_log:info("trace ~s not found~n",
                                                        [rabbit_misc:rs(XN)])
              end
    end.

xname(#basic_message{exchange_name = #resource{name         = XName}}) -> XName.

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

payload(#basic_message{content = #content{payload_fragments_rev = PFR}}) ->
    list_to_binary(lists:reverse(PFR)).

a2b(A) -> list_to_binary(atom_to_list(A)).

type(V) when is_list(V)    -> table;
type(V) when is_integer(V) -> signedint;
type(_V)                   -> longstr.
