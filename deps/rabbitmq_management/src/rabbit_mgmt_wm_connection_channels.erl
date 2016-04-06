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

-module(rabbit_mgmt_wm_connection_channels).

-export([init/1, to_json/2, content_types_provided/2, is_authorized/2]).
-export([finish_request/2, allowed_methods/2]).
-export([encodings_provided/2]).
-export([resource_exists/2]).

-include("rabbit_mgmt.hrl").
-include_lib("webmachine/include/webmachine.hrl").
-include_lib("rabbit_common/include/rabbit.hrl").

%%--------------------------------------------------------------------

init(_Config) -> {ok, #context{}}.

finish_request(ReqData, Context) ->
    {ok, rabbit_mgmt_cors:set_headers(ReqData, ?MODULE), Context}.

allowed_methods(ReqData, Context) ->
    {['HEAD', 'GET', 'OPTIONS'], ReqData, Context}.

content_types_provided(ReqData, Context) ->
   {[{"application/json", to_json}], ReqData, Context}.

encodings_provided(ReqData, Context) ->
    {[{"identity", fun(X) -> X end},
     {"gzip", fun(X) -> zlib:gzip(X) end}], ReqData, Context}.

resource_exists(ReqData, Context) ->
    case rabbit_mgmt_wm_connection:conn(ReqData) of
        error -> {false, ReqData, Context};
        _Conn -> {true, ReqData, Context}
    end.

to_json(ReqData, Context) ->
    Name = proplists:get_value(name, rabbit_mgmt_wm_connection:conn(ReqData)),
    Chs = rabbit_mgmt_db:get_all_channels(rabbit_mgmt_util:range(ReqData)),
    rabbit_mgmt_util:reply_list(
      [Ch || Ch <- rabbit_mgmt_util:filter_conn_ch_list(Chs, ReqData, Context),
             conn_name(Ch) =:= Name],
      ReqData, Context).

is_authorized(ReqData, Context) ->
    try
        rabbit_mgmt_util:is_authorized_user(
          ReqData, Context, rabbit_mgmt_wm_connection:conn(ReqData))
    catch
        {error, invalid_range_parameters, Reason} ->
            rabbit_mgmt_util:bad_request(iolist_to_binary(Reason), ReqData, Context)
    end.

%%--------------------------------------------------------------------

conn_name(Ch) ->
    proplists:get_value(name, proplists:get_value(connection_details, Ch)).
