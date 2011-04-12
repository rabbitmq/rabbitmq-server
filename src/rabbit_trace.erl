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

-export([tap_trace_in/2, tap_trace_out/3]).

-include("rabbit.hrl").
-include("rabbit_framing.hrl").

%%----------------------------------------------------------------------------

-ifdef(use_specs).

-spec(tap_trace_in/2 :: (rabbit_types:basic_message(), rabbit_types:user())
                        -> 'ok').
-spec(tap_trace_out/3 :: (rabbit_amqqueue:qmsg(),
                          rabbit_types:maybe(rabbit_types:ctag()),
                          rabbit_types:user()) -> 'ok').

-endif.

%%----------------------------------------------------------------------------

tap_trace_in(Message = #basic_message{
               exchange_name = #resource{virtual_host = VHost,
                                         name         = XName}}, User) ->
    check_trace(
      XName, VHost,
      fun (TraceExchange) ->
              {EncodedMetadata, Payload} = message_to_table(Message, User),
              publish(TraceExchange, VHost, <<"publish">>, XName,
                      EncodedMetadata, Payload)
      end).

tap_trace_out({#resource{name = QName}, _QPid, _QMsgId, Redelivered,
               Message = #basic_message{
                 exchange_name = #resource{virtual_host = VHost,
                                           name         = XName}}},
              ConsumerTagOrNone, User) ->
    check_trace(
      XName, VHost,
      fun (TraceExchange) ->
              RedeliveredNum = case Redelivered of true -> 1; false -> 0 end,
              {EncodedMetadata, Payload} = message_to_table(Message, User),
              Fields0 = [{<<"redelivered">>, signedint, RedeliveredNum}]
                  ++ EncodedMetadata,
              Fields = case ConsumerTagOrNone of
                           none -> Fields0;
                           CTag -> [{<<"consumer_tag">>, longstr, CTag} |
                                    Fields0]
                       end,
              publish(TraceExchange, VHost, <<"deliver">>, QName,
                      Fields, Payload)
      end).

check_trace(XName, VHost, F) ->
    case application:get_env(rabbit, {trace_exchange, VHost}) of
        undefined    -> ok;
        {ok, XName}  -> ok;
        {ok, TraceX} -> case catch F(TraceX) of
                            {'EXIT', Reason} -> rabbit_log:info(
                                                  "Trace tap died: ~p~n",
                                                  [Reason]);
                            ok               -> ok
                        end
    end.

publish(TraceExchange, VHost, RKPrefix, RKSuffix, Table, Payload) ->
    rabbit_basic:publish(rabbit_misc:r(VHost, exchange, TraceExchange),
                         <<RKPrefix/binary, ".", RKSuffix/binary>>,
                         #'P_basic'{headers = Table}, Payload),
    ok.

message_to_table(#basic_message{exchange_name = #resource{name = XName},
                                routing_keys  = RoutingKeys,
                                content       = Content},
                 #user{username = Username}) ->
    #content{properties            = Props,
             payload_fragments_rev = PFR} =
        rabbit_binary_parser:ensure_content_decoded(Content),
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
    {[{<<"username">>,      longstr, Username},
      {<<"exchange_name">>, longstr, XName},
      {<<"routing_keys">>,  array,   [{longstr, K} || K <- RoutingKeys]},
      {<<"properties">>,    table,   PropsTable},
      {<<"node">>,          longstr, a2b(node())}],
     list_to_binary(lists:reverse(PFR))}.

a2b(A) ->
    list_to_binary(atom_to_list(A)).

type(V) when is_list(V)    -> table;
type(V) when is_integer(V) -> signedint;
type(_V)                   -> longstr.
