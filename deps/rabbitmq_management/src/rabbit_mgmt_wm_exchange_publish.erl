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
%%   The Initial Developer of the Original Code is VMware, Inc.
%%   Copyright (c) 2011-2013 VMware, Inc.  All rights reserved.
%%

-module(rabbit_mgmt_wm_exchange_publish).

-export([init/1, resource_exists/2, post_is_create/2, is_authorized/2,
         allowed_methods/2, process_post/2]).

-include("rabbit_mgmt.hrl").
-include_lib("webmachine/include/webmachine.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").

%%--------------------------------------------------------------------
init(_Config) -> {ok, #context{}}.

allowed_methods(ReqData, Context) ->
    {['POST'], ReqData, Context}.

resource_exists(ReqData, Context) ->
    {case rabbit_mgmt_wm_exchange:exchange(ReqData) of
         not_found -> false;
         _         -> true
     end, ReqData, Context}.

post_is_create(ReqData, Context) ->
    {false, ReqData, Context}.

process_post(ReqData, Context) ->
    rabbit_mgmt_util:post_respond(do_it(ReqData, Context)).

do_it(ReqData, Context) ->
    VHost = rabbit_mgmt_util:vhost(ReqData),
    X = rabbit_mgmt_util:id(exchange, ReqData),
    rabbit_mgmt_util:with_decode(
      [routing_key, properties, payload, payload_encoding], ReqData, Context,
      fun ([RoutingKey, Props0, Payload0, Enc], _) when is_binary(Payload0) ->
              rabbit_mgmt_util:with_channel(
                VHost, ReqData, Context,
                fun (Ch) ->
                        MRef = erlang:monitor(process, Ch),
                        amqp_channel:register_confirm_handler(Ch, self()),
                        amqp_channel:register_return_handler(Ch, self()),
                        amqp_channel:call(Ch, #'confirm.select'{}),
                        Props = rabbit_mgmt_format:to_basic_properties(Props0),
                        Payload = decode(Payload0, Enc),
                        amqp_channel:cast(Ch, #'basic.publish'{
                                            exchange    = X,
                                            routing_key = RoutingKey,
                                            mandatory   = true},
                                          #amqp_msg{props   = Props,
                                                    payload = Payload}),
                        receive
                            {#'basic.return'{}, _} ->
                                receive
                                    #'basic.ack'{} -> ok
                                end,
                                good(MRef, false, ReqData, Context);
                            #'basic.ack'{} ->
                                good(MRef, true, ReqData, Context);
                            {'DOWN', _, _, _, Err} ->
                                bad(Err, ReqData, Context)
                        end
                end);
          ([_RoutingKey, _Props, _Payload, _Enc], _) ->
              throw({error, payload_not_string})
      end).

good(MRef, Routed, ReqData, Context) ->
    erlang:demonitor(MRef),
    rabbit_mgmt_util:reply([{routed, Routed}], ReqData, Context).

bad({shutdown, {connection_closing,
                {server_initiated_close, Code, Reason}}}, ReqData, Context) ->
    rabbit_mgmt_util:bad_request_exception(Code, Reason, ReqData, Context);

bad({shutdown, {server_initiated_close, Code, Reason}}, ReqData, Context) ->
    rabbit_mgmt_util:bad_request_exception(Code, Reason, ReqData, Context).

is_authorized(ReqData, Context) ->
    rabbit_mgmt_util:is_authorized_vhost(ReqData, Context).

%%--------------------------------------------------------------------

decode(Payload, <<"string">>) -> Payload;
decode(Payload, <<"base64">>) -> rabbit_mgmt_util:b64decode_or_throw(Payload);
decode(_Payload, Enc)         -> throw({error, {unsupported_encoding, Enc}}).
