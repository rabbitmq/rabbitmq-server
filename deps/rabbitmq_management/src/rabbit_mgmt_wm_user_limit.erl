%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2020-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_mgmt_wm_user_limit).

-export([init/2,
         content_types_accepted/2, is_authorized/2,
         allowed_methods/2, accept_content/2,
         delete_resource/2]).
-export([variances/2]).

-include_lib("rabbitmq_management_agent/include/rabbit_mgmt_records.hrl").
-include_lib("rabbit_common/include/rabbit.hrl").

%%--------------------------------------------------------------------

init(Req, _State) ->
    {cowboy_rest, rabbit_mgmt_headers:set_common_permission_headers(Req, ?MODULE), #context{}}.

variances(Req, Context) ->
    {[<<"accept-encoding">>, <<"origin">>], Req, Context}.

content_types_accepted(ReqData, Context) ->
   {[{{<<"application">>, <<"json">>, '*'}, accept_content}], ReqData, Context}.

allowed_methods(ReqData, Context) ->
    {[<<"PUT">>, <<"DELETE">>, <<"OPTIONS">>], ReqData, Context}.

accept_content(ReqData0, Context = #context{user = #user{username = ActingUser}}) ->
    case rabbit_mgmt_util:id(user, ReqData0) of
        not_found ->
            rabbit_mgmt_util:not_found(user_not_found, ReqData0, Context);
        Username ->
            rabbit_mgmt_util:with_decode(
                [value], ReqData0, Context,
                fun([Value], _Body, ReqData) ->
                    Limit = #{name(ReqData) => Value},
                    case rabbit_auth_backend_internal:set_user_limits(Username, Limit, ActingUser) of
                        ok ->
                            {true, ReqData, Context};
                        {error_string, Reason} ->
                            rabbit_mgmt_util:bad_request(
                                list_to_binary(Reason), ReqData, Context)
                    end
                end)
    end.

delete_resource(ReqData, Context = #context{user = #user{username = ActingUser}}) ->
    ok = rabbit_auth_backend_internal:clear_user_limits(
            rabbit_mgmt_util:id(user, ReqData), name(ReqData), ActingUser),
    {true, ReqData, Context}.

is_authorized(ReqData, Context) ->
    rabbit_mgmt_util:is_authorized_admin(ReqData, Context).

%%--------------------------------------------------------------------

name(ReqData) -> rabbit_mgmt_util:id(name, ReqData).
