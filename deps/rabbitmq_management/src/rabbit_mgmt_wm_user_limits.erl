%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2020-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_mgmt_wm_user_limits).

-export([init/2, to_json/2, content_types_provided/2,
         resource_exists/2, is_authorized/2, allowed_methods/2]).
-export([variances/2]).

-include_lib("rabbitmq_management_agent/include/rabbit_mgmt_records.hrl").
-include_lib("rabbit_common/include/rabbit.hrl").

%%--------------------------------------------------------------------

init(Req, _State) ->
    {cowboy_rest, rabbit_mgmt_headers:set_common_permission_headers(Req, ?MODULE), #context{}}.

variances(Req, Context) ->
    {[<<"accept-encoding">>, <<"origin">>], Req, Context}.

allowed_methods(ReqData, Context) ->
    {[<<"GET">>, <<"OPTIONS">>], ReqData, Context}.

is_authorized(ReqData, Context) ->
    rabbit_mgmt_util:is_authorized_vhost_visible(ReqData, Context).

content_types_provided(ReqData, Context) ->
   {[{<<"application/json">>, to_json}], ReqData, Context}.

resource_exists(ReqData, Context) ->
   {case user(ReqData) of
        none -> true;
        Username -> rabbit_auth_backend_internal:exists(Username)
    end, ReqData, Context}.

to_json(ReqData, Context) ->
    rabbit_mgmt_util:reply_list(limits(ReqData, Context), [], ReqData, Context).

limits(ReqData, _Context) ->
    case user(ReqData) of
        none ->
            [ [{user, internal_user:get_username(U)}, {value, maps:to_list(internal_user:get_limits(U))}]
                || U <- rabbit_auth_backend_internal:all_users(),
                   internal_user:get_limits(U) =/= #{}];
        Username when is_binary(Username) ->
            case rabbit_auth_backend_internal:get_user_limits(Username) of
                Value when is_map(Value) ->
                    case maps:size(Value) of
                        0 -> [];
                        _ -> [[{user, Username}, {value, maps:to_list(Value)}]]
                    end;
                undefined ->
                    []
            end
    end.
%%--------------------------------------------------------------------

user(ReqData) ->
    rabbit_mgmt_util:id(user, ReqData).
