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
-module(rabbit_mgmt_wm_connection).

-export([init/1, resource_exists/2, to_json/2, content_types_provided/2,
         is_authorized/2, allowed_methods/2, delete_resource/2]).

-include("rabbit_mgmt.hrl").
-include_lib("webmachine/include/webmachine.hrl").
-include_lib("rabbit_common/include/rabbit.hrl").

%%--------------------------------------------------------------------

init(_Config) -> {ok, #context{}}.

content_types_provided(ReqData, Context) ->
   {[{"application/json", to_json}], ReqData, Context}.

allowed_methods(ReqData, Context) ->
    {['HEAD', 'GET', 'DELETE'], ReqData, Context}.

resource_exists(ReqData, Context) ->
    case conn(ReqData) of
        error -> {false, ReqData, Context};
        _Conn -> {true, ReqData, Context}
    end.

to_json(ReqData, Context) ->
    rabbit_mgmt_util:reply({struct, conn(ReqData)}, ReqData, Context).

delete_resource(ReqData, Context) ->
    PidStr = proplists:get_value(pid, conn(ReqData)),
    rabbit_networking:close_connection(
      rabbit_misc:string_to_pid(PidStr), "Closed via management plugin"),
    {true, ReqData, Context}.

is_authorized(ReqData, Context) ->
    rabbit_mgmt_util:is_authorized_user(ReqData, Context, conn(ReqData)).

%%--------------------------------------------------------------------

conn(ReqData) ->
    rabbit_mgmt_db:get_connection(rabbit_mgmt_util:id(connection, ReqData)).
