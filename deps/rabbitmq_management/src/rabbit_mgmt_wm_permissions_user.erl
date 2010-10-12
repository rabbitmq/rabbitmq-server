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
-module(rabbit_mgmt_wm_permissions_user).

-export([init/1, to_json/2, content_types_provided/2, is_authorized/2]).

-include("rabbit_mgmt.hrl").
-include_lib("webmachine/include/webmachine.hrl").
-include_lib("rabbit_common/include/rabbit.hrl").

%%--------------------------------------------------------------------

init(_Config) -> {ok, #context{}}.

content_types_provided(ReqData, Context) ->
   {[{"application/json", to_json}], ReqData, Context}.

to_json(ReqData, Context) ->
    User = rabbit_mgmt_util:id(user, ReqData),
    Perms = rabbit_access_control:list_user_permissions(User),
    rabbit_mgmt_util:reply_list(
      [rabbit_mgmt_format:permissions({User, VHost,
                                       Conf, Write, Read, Scope}) ||
          {VHost, Conf, Write, Read, Scope} <- Perms],
      ["vhost", "user"], ReqData, Context).

is_authorized(ReqData, Context) ->
    rabbit_mgmt_util:is_authorized_admin(ReqData, Context).
