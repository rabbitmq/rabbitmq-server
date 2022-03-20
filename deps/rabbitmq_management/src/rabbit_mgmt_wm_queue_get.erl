%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2011-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_mgmt_wm_queue_get).

-export([init/2, resource_exists/2, is_authorized/2,
  allowed_methods/2, accept_content/2, content_types_provided/2,
  content_types_accepted/2]).
-export([variances/2]).

-include_lib("rabbitmq_management_agent/include/rabbit_mgmt_records.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").

%%--------------------------------------------------------------------

init(Req, _State) ->
    {cowboy_rest, rabbit_mgmt_headers:set_common_permission_headers(Req, ?MODULE), #context{}}.

variances(Req, Context) ->
    {[<<"accept-encoding">>, <<"origin">>], Req, Context}.

allowed_methods(ReqData, Context) ->
    {[<<"POST">>, <<"OPTIONS">>], ReqData, Context}.

content_types_provided(ReqData, Context) ->
   {rabbit_mgmt_util:responder_map(to_json), ReqData, Context}.

resource_exists(ReqData, Context) ->
    {case rabbit_mgmt_wm_queue:queue(ReqData) of
         not_found -> false;
         _         -> true
     end, ReqData, Context}.

content_types_accepted(ReqData, Context) ->
   {[{'*', accept_content}], ReqData, Context}.

accept_content(ReqData, Context) ->
    rabbit_mgmt_util:post_respond(do_it(ReqData, Context)).

do_it(ReqData0, Context) ->
    VHost = rabbit_mgmt_util:vhost(ReqData0),
    Q = rabbit_mgmt_util:id(queue, ReqData0),
    rabbit_mgmt_util:with_decode(
      [ackmode, count, encoding], ReqData0, Context,
      fun([AckModeBin, CountBin, EncBin], Body, ReqData) ->
              rabbit_mgmt_util:with_channel(
                VHost, ReqData, Context,
                fun (Ch) ->
                        AckMode = list_to_atom(binary_to_list(AckModeBin)),
                        Count = rabbit_mgmt_util:parse_int(CountBin),
                        Enc = case EncBin of
                                  <<"auto">>   -> auto;
                                  <<"base64">> -> base64;
                                  _            -> throw({error, <<"Unsupported encoding. Please use auto or base64.">>})
                              end,
                        Trunc = case maps:get(truncate, Body, undefined) of
                                    undefined -> none;
                                    TruncBin  -> rabbit_mgmt_util:parse_int(
                                                   TruncBin)
                                end,

                        Reply = basic_gets(Count, Ch, Q, AckMode, Enc, Trunc),
                        maybe_rejects(Reply, Ch, AckMode),
                        rabbit_mgmt_util:reply(remove_delivery_tag(Reply),
					       ReqData, Context)
                end)
      end).




basic_gets(0, _, _, _, _, _) ->
    [];

basic_gets(Count, Ch, Q, AckMode, Enc, Trunc) ->
    case basic_get(Ch, Q, AckMode, Enc, Trunc) of
        none -> [];
        M    -> [M | basic_gets(Count - 1, Ch, Q, AckMode, Enc, Trunc)]
    end.



ackmode_to_requeue(reject_requeue_false) -> false;
ackmode_to_requeue(reject_requeue_true) -> true.

parse_ackmode(ack_requeue_false) -> true;
parse_ackmode(ack_requeue_true) -> false;
parse_ackmode(reject_requeue_false) -> false;
parse_ackmode(reject_requeue_true) -> false.


% the messages must rejects later,
% because we get always the same message if the
% messages are requeued inside basic_get/5
maybe_rejects(R, Ch, AckMode) ->
    lists:foreach(fun(X) ->
			  maybe_reject(Ch, AckMode,
				       proplists:get_value(delivery_tag, X))
		  end, R).

% removes the delivery_tag from the reply.
% it is not necessary
remove_delivery_tag([])    -> [];
remove_delivery_tag([H|T]) ->
    [proplists:delete(delivery_tag, H) | [X || X <- remove_delivery_tag(T)]].


maybe_reject(Ch, AckMode, DeliveryTag) when AckMode == reject_requeue_true;
					    AckMode == reject_requeue_false ->
    amqp_channel:call(Ch,
		      #'basic.reject'{delivery_tag = DeliveryTag,
				      requeue = ackmode_to_requeue(AckMode)});
maybe_reject(_Ch, _AckMode, _DeliveryTag) -> ok.


basic_get(Ch, Q, AckMode, Enc, Trunc) ->
    case amqp_channel:call(Ch,
			   #'basic.get'{queue = Q,
					no_ack = parse_ackmode(AckMode)}) of
        {#'basic.get_ok'{redelivered   = Redelivered,
                         exchange      = Exchange,
                         routing_key   = RoutingKey,
                         message_count = MessageCount,
                         delivery_tag  = DeliveryTag},
         #amqp_msg{props = Props, payload = Payload}} ->
            [{payload_bytes, size(Payload)},
             {redelivered,   Redelivered},
             {exchange,      Exchange},
             {routing_key,   RoutingKey},
             {message_count, MessageCount},
             {delivery_tag,  DeliveryTag},
             {properties,    rabbit_mgmt_format:basic_properties(Props)}] ++
                payload_part(maybe_truncate(Payload, Trunc), Enc);
        #'basic.get_empty'{} ->
            none
    end.


is_authorized(ReqData, Context) ->
    rabbit_mgmt_util:is_authorized_vhost(ReqData, Context).

%%--------------------------------------------------------------------

maybe_truncate(Payload, none)                         -> Payload;
maybe_truncate(Payload, Len) when size(Payload) < Len -> Payload;
maybe_truncate(Payload, Len) ->
    <<Start:Len/binary, _Rest/binary>> = Payload,
    Start.

payload_part(Payload, Enc) ->
    {PL, E} = case Enc of
                  auto -> case is_utf8(Payload) of
                              true -> {Payload, string};
                              false -> {base64:encode(Payload), base64}
                          end;
                  _    -> {base64:encode(Payload), base64}
              end,
    [{payload, PL}, {payload_encoding, E}].

is_utf8(<<>>) -> true;
is_utf8(<<_/utf8, Rest/bits>>) -> is_utf8(Rest);
is_utf8(_) -> false.
