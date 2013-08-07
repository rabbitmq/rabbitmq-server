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

-module(rabbit_mgmt_wm_queue).

-export([init/1, resource_exists/2, to_json/2,
         content_types_provided/2, content_types_accepted/2,
         is_authorized/2, allowed_methods/2, accept_content/2,
         delete_resource/2, queue/1, queue/2]).

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
    {case queue(ReqData) of
         not_found -> false;
         _         -> true
     end, ReqData, Context}.

to_json(ReqData, Context) ->
    [Q] = rabbit_mgmt_db:augment_queues(
            [queue(ReqData)], rabbit_mgmt_util:range_ceil(ReqData), full),
    rabbit_mgmt_util:reply(rabbit_mgmt_format:strip_pids(Q), ReqData, Context).

accept_content(ReqData, Context) ->
   rabbit_mgmt_util:http_to_amqp(
      'queue.declare', ReqData, Context,
      [{fun rabbit_mgmt_util:parse_bool/1, [durable, auto_delete]}],
      [{queue, rabbit_mgmt_util:id(queue, ReqData)}]).

delete_resource(ReqData, Context) ->
    rabbit_mgmt_util:amqp_request(
      rabbit_mgmt_util:vhost(ReqData),
      ReqData, Context,
      #'queue.delete'{ queue = rabbit_mgmt_util:id(queue, ReqData) }).

is_authorized(ReqData, Context) ->
    rabbit_mgmt_util:is_authorized_vhost(ReqData, Context).

%%--------------------------------------------------------------------

queue(ReqData) ->
    case rabbit_mgmt_util:vhost(ReqData) of
        not_found -> not_found;
        VHost     -> queue(VHost, rabbit_mgmt_util:id(queue, ReqData))
    end.


queue(VHost, QName) ->
    Name = rabbit_misc:r(VHost, queue, QName),
    case rabbit_amqqueue:lookup(Name) of
        {ok, Q}            -> rabbit_mgmt_format:queue(Q);
        {error, not_found} -> not_found
    end.
