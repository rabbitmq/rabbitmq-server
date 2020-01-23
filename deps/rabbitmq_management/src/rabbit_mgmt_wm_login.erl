%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License at
%% https://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
%% License for the specific language governing rights and limitations
%% under the License.
%%
%% The Original Code is RabbitMQ Management Plugin.
%%
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2011-2018 Pivotal Software, Inc.  All rights reserved.
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
            {true, cowboy_req:set_resp_cookie("auth", Value, ReqData1), Context1};
        Other ->
            Other
    end.

accept_content(ReqData, Context) ->
    rabbit_mgmt_util:post_respond(do_login(ReqData, Context)).

do_login(ReqData, Context) ->
    rabbit_mgmt_util:reply(ok, ReqData, Context).
