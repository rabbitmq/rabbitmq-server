%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_mgmt_wm_queue_actions).

-export([init/2, resource_exists/2, is_authorized/2, allow_missing_post/2,
         allowed_methods/2, content_types_accepted/2, accept_content/2]).
-export([variances/2]).

-include_lib("rabbitmq_management_agent/include/rabbit_mgmt_records.hrl").
-include_lib("rabbit/include/amqqueue.hrl").

%%--------------------------------------------------------------------

init(Req, _State) ->
    {cowboy_rest, rabbit_mgmt_headers:set_common_permission_headers(Req, ?MODULE), #context{}}.

variances(Req, Context) ->
    {[<<"accept-encoding">>, <<"origin">>], Req, Context}.

allowed_methods(ReqData, Context) ->
    {[<<"POST">>, <<"OPTIONS">>], ReqData, Context}.

resource_exists(ReqData0, Context) ->
    case rabbit_mgmt_wm_queue:queue(ReqData0) of
        not_found ->
            ReqData1 = rabbit_mgmt_util:set_resp_not_found(<<"queue_not_found">>, ReqData0),
            {false, ReqData1, Context};
        _ ->
            {true, ReqData0, Context}
    end.

allow_missing_post(ReqData, Context) ->
    {false, ReqData, Context}.

content_types_accepted(ReqData, Context) ->
   {[{'*', accept_content}], ReqData, Context}.

accept_content(ReqData, Context) ->
    rabbit_mgmt_util:post_respond(do_it(ReqData, Context)).

do_it(ReqData0, Context) ->
    VHost = rabbit_mgmt_util:vhost(ReqData0),
    QName = rabbit_mgmt_util:id(queue, ReqData0),
    rabbit_mgmt_util:with_decode(
      [action], ReqData0, Context,
      fun([Action], _Body, ReqData) ->
              rabbit_amqqueue:with(
                rabbit_misc:r(VHost, queue, QName),
                fun(Q) -> action(Action, Q, ReqData, Context) end)
      end).

is_authorized(ReqData, Context) ->
    rabbit_mgmt_util:is_authorized_admin(ReqData, Context).

%%--------------------------------------------------------------------

action(Else, _Q, ReqData, Context) ->
    rabbit_mgmt_util:bad_request({unknown, Else}, ReqData, Context).
