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
%%   The Initial Developer of the Original Code is VMware, Inc.
%%   Copyright (c) 2010-2013 VMware, Inc.  All rights reserved.
%%

-module(rabbit_mgmt_wm_connection).

-export([init/1, resource_exists/2, to_json/2, content_types_provided/2,
         is_authorized/2, allowed_methods/2, delete_resource/2, conn/1]).

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
        not_found -> {false, ReqData, Context};
        _Conn     -> {true, ReqData, Context}
    end.

to_json(ReqData, Context) ->
    rabbit_mgmt_util:reply(
      {struct, rabbit_mgmt_format:strip_pids(conn(ReqData))}, ReqData, Context).

delete_resource(ReqData, Context) ->
    Conn = conn(ReqData),
    Pid = proplists:get_value(pid, Conn),
    Reason = case wrq:get_req_header(<<"X-Reason">>, ReqData) of
                 undefined -> "Closed via management plugin";
                 V         -> V
             end,
    case proplists:get_value(type, Conn) of
        direct  -> amqp_direct_connection:server_close(Pid, 320, Reason);
        network -> rabbit_networking:close_connection(Pid, Reason)
    end,
    {true, ReqData, Context}.

is_authorized(ReqData, Context) ->
    rabbit_mgmt_util:is_authorized_user(ReqData, Context, conn(ReqData)).

%%--------------------------------------------------------------------

conn(ReqData) ->
    rabbit_mgmt_db:get_connection(rabbit_mgmt_util:id(connection, ReqData),
                                  rabbit_mgmt_util:range(ReqData)).
