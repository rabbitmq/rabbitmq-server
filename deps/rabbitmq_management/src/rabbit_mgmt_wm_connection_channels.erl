%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_mgmt_wm_connection_channels).

-export([init/2, to_json/2, content_types_provided/2, is_authorized/2]).
-export([resource_exists/2]).
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
