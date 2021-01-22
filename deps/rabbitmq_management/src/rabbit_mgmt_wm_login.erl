%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2011-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_mgmt_wm_login).

-export([init/2, is_authorized/2,
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

content_types_accepted(ReqData, Context) ->
    {[{{<<"application">>, <<"x-www-form-urlencoded">>, '*'}, accept_content}], ReqData, Context}.

is_authorized(#{method := <<"OPTIONS">>} = ReqData, Context) ->
    {true, ReqData, Context};
is_authorized(ReqData0, Context) ->
    {ok, Body, ReqData} = cowboy_req:read_urlencoded_body(ReqData0),
    Username = proplists:get_value(<<"username">>, Body),
    Password = proplists:get_value(<<"password">>, Body),
    case rabbit_mgmt_util:is_authorized_user(ReqData, Context, Username, Password) of
        {true, ReqData1, Context1} ->
            Value = base64:encode(<<Username/binary,":",Password/binary>>),
            {true, cowboy_req:set_resp_cookie(<<"auth">>, Value, ReqData1), Context1};
        Other ->
            Other
    end.

accept_content(ReqData, Context) ->
    rabbit_mgmt_util:post_respond(do_login(ReqData, Context)).

do_login(ReqData, Context) ->
    rabbit_mgmt_util:reply(ok, ReqData, Context).
