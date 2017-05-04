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
%%   The Initial Developer of the Original Code is GoPivotal, Inc.
%%   Copyright (c) 2007-2017 Pivotal Software, Inc.  All rights reserved.
%%

-module(rabbit_mgmt_wm_permission).

-export([init/3, rest_init/2, resource_exists/2, to_json/2,
         content_types_provided/2, content_types_accepted/2,
         is_authorized/2, allowed_methods/2, accept_content/2,
         delete_resource/2]).
-export([variances/2]).

-include_lib("rabbitmq_management_agent/include/rabbit_mgmt_records.hrl").
-include_lib("rabbit_common/include/rabbit.hrl").

%%--------------------------------------------------------------------

init(_, _, _) -> {upgrade, protocol, cowboy_rest}.

rest_init(Req, _Config) ->
    {ok, rabbit_mgmt_cors:set_headers(Req, ?MODULE), #context{}}.

variances(Req, Context) ->
    {[<<"accept-encoding">>, <<"origin">>], Req, Context}.

content_types_provided(ReqData, Context) ->
   {rabbit_mgmt_util:responder_map(to_json), ReqData, Context}.

content_types_accepted(ReqData, Context) ->
    {[{'*', accept_content}], ReqData, Context}.

allowed_methods(ReqData, Context) ->
    {[<<"HEAD">>, <<"GET">>, <<"PUT">>, <<"DELETE">>, <<"OPTIONS">>], ReqData, Context}.

resource_exists(ReqData, Context) ->
    {case perms(ReqData) of
         none      -> false;
         not_found -> false;
         _         -> true
     end, ReqData, Context}.

to_json(ReqData, Context) ->
    rabbit_mgmt_util:reply(perms(ReqData), ReqData, Context).

accept_content(ReqData0, Context) ->
    case perms(ReqData0) of
         not_found ->
            rabbit_mgmt_util:bad_request(vhost_or_user_not_found,
                                         ReqData0, Context);
         _         ->
            User = rabbit_mgmt_util:id(user, ReqData0),
            VHost = rabbit_mgmt_util:id(vhost, ReqData0),
            rabbit_mgmt_util:with_decode(
              [configure, write, read], ReqData0, Context,
              fun([Conf, Write, Read], _, ReqData) ->
                      rabbit_auth_backend_internal:set_permissions(
                        User, VHost, Conf, Write, Read),
                      {true, ReqData, Context}
              end)
    end.

delete_resource(ReqData, Context) ->
    User = rabbit_mgmt_util:id(user, ReqData),
    VHost = rabbit_mgmt_util:id(vhost, ReqData),
    rabbit_auth_backend_internal:clear_permissions(User, VHost),
    {true, ReqData, Context}.

is_authorized(ReqData, Context) ->
    rabbit_mgmt_util:is_authorized_admin(ReqData, Context).

%%--------------------------------------------------------------------

perms(ReqData) ->
    User = rabbit_mgmt_util:id(user, ReqData),
    case rabbit_auth_backend_internal:lookup_user(User) of
        {ok, _} ->
            case rabbit_mgmt_util:vhost(ReqData) of
                not_found ->
                    not_found;
                VHost ->
                    Perms =
                        rabbit_auth_backend_internal:list_user_vhost_permissions(
                          User, VHost),
                    case Perms of
                        [Rest] -> [{user,  User},
                                   {vhost, VHost} | Rest];
                        []     -> none
                    end
            end;
        {error, _} ->
            not_found
    end.
