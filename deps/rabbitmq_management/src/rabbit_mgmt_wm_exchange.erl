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
%%   The Original Code is RabbitMQ Management Console.
%%
%%   The Initial Developers of the Original Code are Rabbit Technologies Ltd.
%%
%%   Copyright (C) 2010 Rabbit Technologies Ltd.
%%
%%   All Rights Reserved.
%%
%%   Contributor(s): ______________________________________.
%%
-module(rabbit_mgmt_wm_exchange).

-export([init/1, resource_exists/2, to_json/2,
         content_types_provided/2, content_types_accepted/2,
         is_authorized/2, allowed_methods/2, accept_content/2,
         delete_resource/2]).

-include("rabbit_mgmt.hrl").
-include_lib("webmachine/include/webmachine.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").

%%--------------------------------------------------------------------
init(_Config) -> {ok, #context{}}.

content_types_provided(ReqData, Context) ->
   {[{"application/json", to_json}], ReqData, Context}.

content_types_accepted(ReqData, Context) ->
   {[{"application/json", accept_content}], ReqData, Context}.

allowed_methods(ReqData, Context) ->
    {['HEAD', 'GET', 'PUT', 'DELETE'], ReqData, Context}.

resource_exists(ReqData, Context) ->
    {case exchange(ReqData) of
         not_found -> false;
         _         -> true
     end, ReqData, Context}.

to_json(ReqData, Context) ->
    rabbit_mgmt_util:reply(
      rabbit_mgmt_format:exchange(rabbit_exchange:info(exchange(ReqData))),
      ReqData, Context).

accept_content(ReqData, Context) ->
    Name = rabbit_mgmt_util:id(exchange, ReqData),
    rabbit_mgmt_util:with_decode_vhost(
      [type, durable, auto_delete, arguments], ReqData, Context,
      fun(VHost, [Type, Durable, AutoDelete, Args]) ->
              Durable1    = rabbit_mgmt_util:parse_bool(Durable),
              AutoDelete1 = rabbit_mgmt_util:parse_bool(AutoDelete),
              rabbit_mgmt_util:amqp_request(
                VHost, ReqData, Context,
                #'exchange.declare'{ exchange    = Name,
                                     type        = Type,
                                     durable     = Durable1,
                                     auto_delete = AutoDelete1,
                                     arguments   = rabbit_mgmt_util:args(Args)})
      end).

delete_resource(ReqData, Context) ->
    rabbit_mgmt_util:amqp_request(
      rabbit_mgmt_util:vhost(ReqData), ReqData, Context,
      #'exchange.delete'{ exchange = id(ReqData) }).

is_authorized(ReqData, Context) ->
    rabbit_mgmt_util:is_authorized_vhost(ReqData, Context).

%%--------------------------------------------------------------------

exchange(ReqData) ->
    case rabbit_mgmt_util:vhost(ReqData) of
        not_found -> not_found;
        VHost     -> Name = rabbit_misc:r(VHost, exchange, id(ReqData)),
                     case rabbit_exchange:lookup(Name) of
                         {ok, X}            -> X;
                         {error, not_found} -> not_found
                     end
    end.

id(ReqData) ->
    rabbit_mgmt_util:id(exchange, ReqData).
