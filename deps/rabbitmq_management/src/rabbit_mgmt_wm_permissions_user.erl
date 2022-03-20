%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_mgmt_wm_permissions_user).

-export([init/2, to_json/2, content_types_provided/2, resource_exists/2,
         is_authorized/2]).
-export([variances/2]).

-include_lib("rabbitmq_management_agent/include/rabbit_mgmt_records.hrl").
-include_lib("rabbit_common/include/rabbit.hrl").

%%--------------------------------------------------------------------

init(Req, _State) ->
    {cowboy_rest, rabbit_mgmt_headers:set_common_permission_headers(Req, ?MODULE), #context{}}.

variances(Req, Context) ->
    {[<<"accept-encoding">>, <<"origin">>], Req, Context}.

content_types_provided(ReqData, Context) ->
   {rabbit_mgmt_util:responder_map(to_json), ReqData, Context}.

resource_exists(ReqData, Context) ->
    {case rabbit_mgmt_wm_user:user(ReqData) of
         {ok, _}    -> true;
         {error, _} -> false
     end, ReqData, Context}.

to_json(ReqData, Context) ->
    User = rabbit_mgmt_util:id(user, ReqData),
    rabbit_mgmt_util:catch_no_such_user_or_vhost(
        fun() ->
            Perms = rabbit_auth_backend_internal:list_user_permissions(User),
            rabbit_mgmt_util:reply_list([[{user, User} | Rest] || Rest <- Perms],
                                        ["vhost", "user"], ReqData, Context)
        end,
        fun() ->
            rabbit_mgmt_util:bad_request(vhost_or_user_not_found, ReqData, Context)
        end).

is_authorized(ReqData, Context) ->
    rabbit_mgmt_util:is_authorized_admin(ReqData, Context).
