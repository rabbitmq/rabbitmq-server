%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_mgmt_wm_user).

-export([init/2, resource_exists/2, to_json/2,
         content_types_provided/2, content_types_accepted/2,
         is_authorized/2, allowed_methods/2, accept_content/2,
         delete_resource/2, user/1, put_user/2, put_user/3]).
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

content_types_accepted(ReqData, Context) ->
    {[{'*', accept_content}], ReqData, Context}.

allowed_methods(ReqData, Context) ->
    {[<<"HEAD">>, <<"GET">>, <<"PUT">>, <<"DELETE">>, <<"OPTIONS">>], ReqData, Context}.

resource_exists(ReqData, Context) ->
    {case user(ReqData) of
         {ok, _}    -> true;
         {error, _} -> false
     end, ReqData, Context}.

to_json(ReqData, Context) ->
    {ok, User} = user(ReqData),
    rabbit_mgmt_util:reply(rabbit_mgmt_format:internal_user(User),
                           ReqData, Context).

accept_content(ReqData0, Context = #context{user = #user{username = ActingUser}}) ->
    Username = rabbit_mgmt_util:id(user, ReqData0),
    rabbit_mgmt_util:with_decode(
      [], ReqData0, Context,
      fun(_, User, ReqData) ->
              put_user(User#{name => Username}, ActingUser),
              {true, ReqData, Context}
      end).

delete_resource(ReqData, Context = #context{user = #user{username = ActingUser}}) ->
    User = rabbit_mgmt_util:id(user, ReqData),
    rabbit_auth_backend_internal:delete_user(User, ActingUser),
    {true, ReqData, Context}.

is_authorized(ReqData, Context) ->
    rabbit_mgmt_util:is_authorized_admin(ReqData, Context).

%%--------------------------------------------------------------------

user(ReqData) ->
    rabbit_auth_backend_internal:lookup_user(rabbit_mgmt_util:id(user, ReqData)).

put_user(User, ActingUser) ->
    put_user(User, undefined, ActingUser).

put_user(User, Version, ActingUser) ->
    rabbit_auth_backend_internal:put_user(User, Version, ActingUser).
