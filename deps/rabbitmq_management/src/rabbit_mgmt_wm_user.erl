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
%%   The Original Code is RabbitMQ Management Console.
%%
%%   The Initial Developers of the Original Code are Rabbit Technologies Ltd.
%%
%%   Copyright (C) 2010 Rabbit Technologies Ltd.
%%
%%   All Rights Reserved.
%%
%%   Contributor(s): ______________________________________.
%%
-module(rabbit_mgmt_wm_user).

-export([init/1, resource_exists/2, to_json/2,
         content_types_provided/2, content_types_accepted/2,
         is_authorized/2, allowed_methods/2, accept_content/2,
         delete_resource/2]).

-include("rabbit_mgmt.hrl").
-include_lib("webmachine/include/webmachine.hrl").
-include_lib("rabbit_common/include/rabbit.hrl").

%%--------------------------------------------------------------------
init(_Config) -> {ok, #context{}}.

content_types_provided(ReqData, Context) ->
   {[{"application/json", to_json}], ReqData, Context}.

content_types_accepted(ReqData, Context) ->
   {[{"application/json", accept_content}], ReqData, Context}.

allowed_methods(ReqData, Context) ->
    {['HEAD', 'GET', 'PUT', 'DELETE'], ReqData, Context}.

resource_exists(ReqData, Context) ->
    {case user(ReqData) of
         {ok, _}    -> true;
         {error, _} -> false
     end, ReqData, Context}.

to_json(ReqData, Context) ->
    {ok, User} = user(ReqData),
    rabbit_mgmt_util:reply(rabbit_mgmt_format:user(User), ReqData, Context).

accept_content(ReqData, Context) ->
    User = rabbit_mgmt_util:id(user, ReqData),
    rabbit_mgmt_util:with_decode(
      [password, administrator], ReqData, Context,
      fun([Password, IsAdmin]) ->
              case rabbit_access_control:lookup_user(User) of
                  {ok, _} ->
                      rabbit_access_control:change_password(User, Password),
                      case rabbit_mgmt_util:parse_bool(IsAdmin) of
                          true ->  rabbit_access_control:set_admin(User);
                          false -> rabbit_access_control:clear_admin(User)
                      end;
                  {error, not_found} ->
                      rabbit_access_control:add_user(User, Password)
              end,
              {true, ReqData, Context}
      end).

delete_resource(ReqData, Context) ->
    User = rabbit_mgmt_util:id(user, ReqData),
    rabbit_access_control:delete_user(User),
    {true, ReqData, Context}.

is_authorized(ReqData, Context) ->
    rabbit_mgmt_util:is_authorized(ReqData, Context).

%%--------------------------------------------------------------------

user(ReqData) ->
    rabbit_access_control:lookup_user(rabbit_mgmt_util:id(user, ReqData)).
