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
%%   Copyright (c) 2011-2012 VMware, Inc.  All rights reserved.
%%

-module(rabbit_mgmt_wm_queue_actions).

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
    {case rabbit_mgmt_wm_queue:queue(ReqData) of
         not_found -> false;
         _         -> true
     end, ReqData, Context}.

post_is_create(ReqData, Context) ->
    {false, ReqData, Context}.

process_post(ReqData, Context) ->
    rabbit_mgmt_util:post_respond(do_it(ReqData, Context)).

do_it(ReqData, Context) ->
    VHost = rabbit_mgmt_util:vhost(ReqData),
    QName = rabbit_mgmt_util:id(queue, ReqData),
    rabbit_mgmt_util:with_decode(
      [action], ReqData, Context,
      fun([ActionBin], _Body) ->
              action(ActionBin, QName, VHost, ReqData, Context)
      end).

is_authorized(ReqData, Context) ->
    rabbit_mgmt_util:is_authorized_admin(ReqData, Context).

%%--------------------------------------------------------------------

action(<<"sync">>, QName, VHost, ReqData, Context) ->
    spawn(fun() -> rabbit_amqqueue:sync(QName, VHost) end),
    {true, ReqData, Context};

action(<<"cancel_sync">>, QName, VHost, ReqData, Context) ->
    rabbit_amqqueue:cancel_sync(QName, VHost),
    {true, ReqData, Context};

action(Else, _QName, _VHost, ReqData, Context) ->
    rabbit_mgmt_util:bad_request({unknown, Else}, ReqData, Context).
