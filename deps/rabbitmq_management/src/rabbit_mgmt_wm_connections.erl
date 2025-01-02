%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_mgmt_wm_connections).

-export([init/2, to_json/2, content_types_provided/2, is_authorized/2,
         augmented/2]).
-export([variances/2]).

-import(rabbit_misc, [pget/2]).

-include_lib("rabbitmq_management_agent/include/rabbit_mgmt_records.hrl").

%%--------------------------------------------------------------------

init(Req, _State) ->
    {cowboy_rest, rabbit_mgmt_headers:set_common_permission_headers(Req, ?MODULE), #context{}}.

variances(Req, Context) ->
    {[<<"accept-encoding">>, <<"origin">>], Req, Context}.

content_types_provided(ReqData, Context) ->
   {rabbit_mgmt_util:responder_map(to_json), ReqData, Context}.

to_json(ReqData, Context) ->
    try
        Connections = do_connections_query(ReqData, Context),
        rabbit_mgmt_util:reply_list_or_paginate(Connections, ReqData, Context)
    catch
        {error, invalid_range_parameters, Reason} ->
            rabbit_mgmt_util:bad_request(iolist_to_binary(Reason), ReqData, Context)
    end.

is_authorized(ReqData, Context) ->
    rabbit_mgmt_util:is_authorized(ReqData, Context).

augmented(ReqData, Context) ->
    rabbit_mgmt_util:filter_conn_ch_list(
      rabbit_mgmt_db:get_all_connections(
        rabbit_mgmt_util:range_ceil(ReqData)), ReqData, Context).

do_connections_query(ReqData, Context) ->
    case rabbit_mgmt_util:disable_stats(ReqData) of
        false ->
            augmented(ReqData, Context);
        true ->
            rabbit_mgmt_util:filter_tracked_conn_list(rabbit_connection_tracking:list(),
                                                      ReqData, Context)
    end.
