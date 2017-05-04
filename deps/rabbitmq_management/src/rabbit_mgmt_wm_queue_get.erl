%%   The contents of this file are subject to the Mozilla Public License
%%   Version 1.1 (the "License"); you may not use this file except in
%%   compliance with the License. You may obtain a copy of the License at
%%   http://www.mozilla.org/MPL/
%%
%%   Software distributed under the License is distributed on an "AS IS"
%%   basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
%%   License for the specific language governing rights and limitations
%%   under the License.
%%
%%   The Original Code is RabbitMQ Management Plugin.
%%
%%   The Initial Developer of the Original Code is GoPivotal, Inc.
%%   Copyright (c) 2011-2015 Pivotal Software, Inc.  All rights reserved.
%%

-module(rabbit_mgmt_wm_queue_get).

-export([init/3, rest_init/2, resource_exists/2, is_authorized/2,
  allowed_methods/2, accept_content/2, content_types_provided/2,
  content_types_accepted/2]).
-export([variances/2]).

-include_lib("rabbitmq_management_agent/include/rabbit_mgmt_records.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").

%%--------------------------------------------------------------------

init(_, _, _) -> {upgrade, protocol, cowboy_rest}.

rest_init(Req, _Config) ->
    {ok, rabbit_mgmt_cors:set_headers(Req, ?MODULE), #context{}}.

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
      [requeue, count, encoding], ReqData0, Context,
      fun([RequeueBin, CountBin, EncBin], Body, ReqData) ->
              rabbit_mgmt_util:with_channel(
                VHost, ReqData, Context,
                fun (Ch) ->
                        NoAck = not rabbit_mgmt_util:parse_bool(RequeueBin),
                        Count = rabbit_mgmt_util:parse_int(CountBin),
                        Enc = case EncBin of
                                  <<"auto">>   -> auto;
                                  <<"base64">> -> base64;
                                  _            -> throw({error, <<"Unsupported encoding. Please use auto or base64.">>})
                              end,
                        Trunc = case proplists:get_value(truncate, Body) of
                                    undefined -> none;
                                    TruncBin  -> rabbit_mgmt_util:parse_int(
                                                   TruncBin)
                                end,
                        rabbit_mgmt_util:reply(
                          basic_gets(Count, Ch, Q, NoAck, Enc, Trunc),
                          ReqData, Context)
                end)
      end).

basic_gets(0, _, _, _, _, _) ->
    [];

basic_gets(Count, Ch, Q, NoAck, Enc, Trunc) ->
    case basic_get(Ch, Q, NoAck, Enc, Trunc) of
        none -> [];
        M    -> [M | basic_gets(Count - 1, Ch, Q, NoAck, Enc, Trunc)]
    end.

basic_get(Ch, Q, NoAck, Enc, Trunc) ->
    case amqp_channel:call(Ch, #'basic.get'{queue = Q,
                                            no_ack = NoAck}) of
        {#'basic.get_ok'{redelivered   = Redelivered,
                         exchange      = Exchange,
                         routing_key   = RoutingKey,
                         message_count = MessageCount},
         #amqp_msg{props = Props, payload = Payload}} ->
            [{payload_bytes, size(Payload)},
             {redelivered,   Redelivered},
             {exchange,      Exchange},
             {routing_key,   RoutingKey},
             {message_count, MessageCount},
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
                  auto -> try
                              %% TODO mochijson does this but is it safe?
                              _ = xmerl_ucs:from_utf8(Payload),
                              {Payload, string}
                          catch exit:{ucs, _} ->
                                  {base64:encode(Payload), base64}
                          end;
                  _    -> {base64:encode(Payload), base64}
              end,
    [{payload, PL}, {payload_encoding, E}].
