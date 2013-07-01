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
%%   Copyright (c) 2010-2013 GoPivotal, Inc.  All rights reserved.
%%

-module(rabbit_mgmt_wm_exchange).

-export([init/1, resource_exists/2, to_json/2,
         content_types_provided/2, content_types_accepted/2,
         is_authorized/2, allowed_methods/2, accept_content/2,
         delete_resource/2, exchange/1, exchange/2]).

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
    [X] = rabbit_mgmt_db:augment_exchanges(
            [exchange(ReqData)], rabbit_mgmt_util:range(ReqData), full),
    rabbit_mgmt_util:reply(X, ReqData, Context).

accept_content(ReqData, Context) ->
    rabbit_mgmt_util:http_to_amqp(
      'exchange.declare', ReqData, Context,
      [{fun rabbit_mgmt_util:parse_bool/1, [durable, auto_delete, internal]}],
      [{exchange, rabbit_mgmt_util:id(exchange, ReqData)}]).

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
        VHost     -> exchange(VHost, id(ReqData))
    end.

exchange(VHost, XName) ->
    Name = rabbit_misc:r(VHost, exchange, XName),
    case rabbit_exchange:lookup(Name) of
        {ok, X}            -> rabbit_mgmt_format:exchange(
                                rabbit_exchange:info(X));
        {error, not_found} -> not_found
    end.

id(ReqData) ->
    rabbit_mgmt_util:id(exchange, ReqData).
