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

-export([tap_trace_in/1, tap_trace_out/3]).

-include("rabbit.hrl").
-include("rabbit_framing.hrl").

%%----------------------------------------------------------------------------

-ifdef(use_specs).

%% TODO

-endif.

%%----------------------------------------------------------------------------

tap_trace_in(Message = #basic_message{
               exchange_name = #resource{virtual_host = VHostBin,
                                         name         = XNameBin}}) ->
    check_trace(
      XNameBin,
      VHostBin,
      fun (TraceExchangeBin) ->
              {EncodedMetadata, Payload} = message_to_table(Message),
              publish(TraceExchangeBin, VHostBin, <<"publish">>, XNameBin,
                      EncodedMetadata, Payload)
      end).

tap_trace_out({#resource{name = QNameBin}, _QPid, _QMsgId, Redelivered,
               Message = #basic_message{
                 exchange_name = #resource{virtual_host = VHostBin,
                                           name         = XNameBin}}},
              DeliveryTag,
              ConsumerTagOrNone) ->
    check_trace(
      XNameBin,
      VHostBin,
      fun (TraceExchangeBin) ->
              RedeliveredNum = case Redelivered of true -> 1; false -> 0 end,
              {EncodedMetadata, Payload} = message_to_table(Message),
              Fields0 = [{<<"delivery_tag">>, signedint, DeliveryTag},
                         {<<"redelivered">>,  signedint, RedeliveredNum}]
                  ++ EncodedMetadata,
              Fields = case ConsumerTagOrNone of
                           none -> Fields0;
                           CTag -> [{<<"consumer_tag">>, longstr, CTag} |
                                    Fields0]
                       end,
              publish(TraceExchangeBin, VHostBin, <<"deliver">>, QNameBin,
                      Fields, Payload)
      end).

check_trace(XNameBin, VHostBin, F) ->
    case catch case application:get_env(rabbit, {trace_exchange, VHostBin}) of
                   undefined              -> ok;
                   {ok, XNameBin}         -> ok;
                   {ok, TraceExchangeBin} -> F(TraceExchangeBin)
               end of
        {'EXIT', Reason} -> rabbit_log:info("Trace tap died: ~p~n", [Reason]);
        ok               -> ok
    end.

publish(TraceExchangeBin, VHostBin, RKPrefix, RKSuffix, Table, Payload) ->
    rabbit_basic:publish(rabbit_misc:r(VHostBin, exchange, TraceExchangeBin),
                         <<RKPrefix/binary, ".", RKSuffix/binary>>,
                         #'P_basic'{headers = Table}, Payload),
    ok.

message_to_table(#basic_message{exchange_name = #resource{name = XName},
                                routing_keys = RoutingKeys,
                                content = Content}) ->
    #content{properties = #'P_basic'{content_type     = ContentType,
                                     content_encoding = ContentEncoding,
                                     headers          = Headers,
                                     delivery_mode    = DeliveryMode,
                                     priority         = Priority,
                                     correlation_id   = CorrelationId,
                                     reply_to         = ReplyTo,
                                     expiration       = Expiration,
                                     message_id       = MessageId,
                                     timestamp        = Timestamp,
                                     type             = Type,
                                     user_id          = UserId,
                                     app_id           = AppId},
             payload_fragments_rev = PFR} =
        rabbit_binary_parser:ensure_content_decoded(Content),
    Headers1 = prune_undefined(
                [{<<"content_type">>,     longstr,   ContentType},
                 {<<"content_encoding">>, longstr,   ContentEncoding},
                 {<<"headers">>,          table,     Headers},
                 {<<"delivery_mode">>,    signedint, DeliveryMode},
                 {<<"priority">>,         signedint, Priority},
                 {<<"correlation_id">>,   longstr,   CorrelationId},
                 {<<"reply_to">>,         longstr,   ReplyTo},
                 {<<"expiration">>,       longstr,   Expiration},
                 {<<"message_id">>,       longstr,   MessageId},
                 {<<"timestamp">>,        longstr,   Timestamp},
                 {<<"type">>,             longstr,   Type},
                 {<<"user_id">>,          longstr,   UserId},
                 {<<"app_id">>,           longstr,   AppId}]),
    {[{<<"exchange_name">>, longstr, XName},
      {<<"routing_keys">>,  array,   [{longstr, K} || K <- RoutingKeys]},
      {<<"headers">>,       table,   Headers1},
      {<<"node">>,          longstr, list_to_binary(atom_to_list(node()))}],
     list_to_binary(lists:reverse(PFR))}.

prune_undefined(Fields) ->
    [F || F = {_, _, Value} <- Fields,
          Value =/= undefined].
