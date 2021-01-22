%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_mgmt_wm_topic_permission).

-export([init/2, resource_exists/2, to_json/2,
         content_types_provided/2, content_types_accepted/2,
         is_authorized/2, allowed_methods/2, accept_content/2,
         delete_resource/2]).
-export([variances/2]).

-include_lib("rabbitmq_management_agent/include/rabbit_mgmt_records.hrl").
-include_lib("rabbit_common/include/rabbit.hrl").

%%--------------------------------------------------------------------

init(Req, _State) ->
    {cowboy_rest, rabbit_mgmt_headers:set_common_permission_headers(Req, ?MODULE), #context{}}.

variances(Req, Context) ->
    {[<<"accept-encoding">>, <<"origin">>], Req, Context}.

content_types_provided(ReqData, Context) ->
   {[{<<"application/json">>, to_json}], ReqData, Context}.

content_types_accepted(ReqData, Context) ->
    {[{'*', accept_content}], ReqData, Context}.

allowed_methods(ReqData, Context) ->
    {[<<"HEAD">>, <<"GET">>, <<"PUT">>, <<"DELETE">>, <<"OPTIONS">>], ReqData, Context}.

resource_exists(ReqData, Context) ->
    {case topic_perms(ReqData) of
         none      -> false;
         not_found -> false;
         _         -> true
     end, ReqData, Context}.

to_json(ReqData, Context) ->
    rabbit_mgmt_util:reply(topic_perms(ReqData), ReqData, Context).

accept_content(ReqData0, Context = #context{user = #user{username = Username}}) ->
    case topic_perms(ReqData0) of
         not_found ->
            rabbit_mgmt_util:bad_request(vhost_or_user_not_found,
                                         ReqData0, Context);
         _         ->
            User = rabbit_mgmt_util:id(user, ReqData0),
            VHost = rabbit_mgmt_util:id(vhost, ReqData0),
            rabbit_mgmt_util:with_decode(
              [exchange, write, read], ReqData0, Context,
              fun([Exchange, Write, Read], _, ReqData) ->
                      rabbit_auth_backend_internal:set_topic_permissions(
                        User, VHost, Exchange, Write, Read, Username),
                      {true, ReqData, Context}
              end)
    end.

delete_resource(ReqData, Context = #context{user = #user{username = Username}}) ->
    User = rabbit_mgmt_util:id(user, ReqData),
    VHost = rabbit_mgmt_util:id(vhost, ReqData),
    case rabbit_mgmt_util:id(exchange, ReqData) of
        none ->
            rabbit_auth_backend_internal:clear_topic_permissions(User, VHost, Username);
        Exchange ->
            rabbit_auth_backend_internal:clear_topic_permissions(User, VHost, Exchange,
                                                                 Username)
    end,
    {true, ReqData, Context}.

is_authorized(ReqData, Context) ->
    rabbit_mgmt_util:is_authorized_admin(ReqData, Context).

%%--------------------------------------------------------------------

topic_perms(ReqData) ->
    User = rabbit_mgmt_util:id(user, ReqData),
    case rabbit_auth_backend_internal:lookup_user(User) of
        {ok, _} ->
            case rabbit_mgmt_util:vhost(ReqData) of
                not_found ->
                    not_found;
                VHost ->
                    rabbit_mgmt_util:catch_no_such_user_or_vhost(
                        fun() ->
                            Perms =
                                rabbit_auth_backend_internal:list_user_vhost_topic_permissions(
                                  User, VHost),
                            case Perms of
                                []     -> none;
                                TopicPermissions -> [[{user, User}, {vhost, VHost} | TopicPermission]
                                || TopicPermission <- TopicPermissions]
                            end
                        end,
                        fun() -> not_found end)
            end;
        {error, _} ->
            not_found
    end.
