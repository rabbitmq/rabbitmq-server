%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2011-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_mgmt_login).

-export([init/2, is_authorized/2,
         allowed_methods/2, accept_content/2,
         content_types_accepted/2]).
-export([variances/2]).

-include_lib("rabbitmq_management_agent/include/rabbit_mgmt_records.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").

%%--------------------------------------------------------------------

init(Req, _State) ->
    {cowboy_rest, rabbit_mgmt_headers:set_common_permission_headers(Req, ?MODULE),  #context{}}.

variances(Req, Context) ->
    {[<<"accept-encoding">>, <<"origin">>], Req, Context}.

allowed_methods(ReqData, Context) ->
    {[<<"POST">>, <<"OPTIONS">>], ReqData, Context}.

content_types_accepted(ReqData, Context) ->
    {[{{<<"application">>, <<"x-www-form-urlencoded">>, '*'}, accept_content}], ReqData, Context}.

is_authorized(#{method := <<"OPTIONS">>} = ReqData, Context) ->
    {true, ReqData, Context};
is_authorized(ReqData0, Context) ->
    {ok, Body, ReqData} = cowboy_req:read_urlencoded_body(ReqData0),
    AccessToken = proplists:get_value(<<"access_token">>, Body),
    case rabbit_mgmt_util:is_authorized_user(ReqData, Context, <<"">>, AccessToken, false) of
        {true, ReqData1, Context1} ->
            Value = "2258:" ++ AccessToken, %% 2258 is the short name for auth cookie embeded into the real cookie m
            {true, cowboy_req:set_resp_cookie(<<"m">>, Value, ReqData1), Context1};
        {false, ReqData1, Reason} ->
            {true, cowboy_req:set_resp_cookie(<<"m">>, "", ReqData1), Context#context{impl = "error=" ++ Reason}}
    end.

accept_content(ReqData, Context = #context{impl = Status})  ->
    rabbit_log:info("accept_context when succeedeed"),
    rabbit_mgmt_util:post_respond(rabbit_mgmt_util:redirect_to_home(ReqData, Status, Context)).
