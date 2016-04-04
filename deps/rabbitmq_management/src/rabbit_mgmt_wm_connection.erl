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
%%   Copyright (c) 2007-2016 Pivotal Software, Inc.  All rights reserved.
%%

-module(rabbit_mgmt_wm_connection).

-export([init/1, resource_exists/2, to_json/2, content_types_provided/2,
         is_authorized/2, allowed_methods/2, delete_resource/2, conn/1]).
-export([finish_request/2]).
-export([encodings_provided/2]).

-include("rabbit_mgmt.hrl").
-include_lib("webmachine/include/webmachine.hrl").
-include_lib("rabbit_common/include/rabbit.hrl").

%%--------------------------------------------------------------------

init(_Config) -> {ok, #context{}}.

finish_request(ReqData, Context) ->
    {ok, rabbit_mgmt_cors:set_headers(ReqData, ?MODULE), Context}.

content_types_provided(ReqData, Context) ->
   {[{"application/json", to_json}], ReqData, Context}.

encodings_provided(ReqData, Context) ->
    {[{"identity", fun(X) -> X end},
     {"gzip", fun(X) -> zlib:gzip(X) end}], ReqData, Context}.

allowed_methods(ReqData, Context) ->
    {['HEAD', 'GET', 'DELETE', 'OPTIONS'], ReqData, Context}.

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
    try
        rabbit_mgmt_util:is_authorized_user(ReqData, Context, conn(ReqData))
    catch
        {error, invalid_range_parameters, Reason} ->
            rabbit_mgmt_util:bad_request(iolist_to_binary(Reason), ReqData, Context)
    end.

%%--------------------------------------------------------------------

conn(ReqData) ->
    rabbit_mgmt_db:get_connection(rabbit_mgmt_util:id(connection, ReqData),
                                  rabbit_mgmt_util:range_ceil(ReqData)).
