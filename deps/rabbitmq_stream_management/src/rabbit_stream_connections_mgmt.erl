%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2020-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_stream_connections_mgmt).

-behaviour(rabbit_mgmt_extension).

-export([dispatcher/0,
         web_ui/0]).
-export([init/2,
         to_json/2,
         content_types_provided/2,
         is_authorized/2]).

-include_lib("rabbitmq_management_agent/include/rabbit_mgmt_records.hrl").

dispatcher() ->
  case rabbit_stream_management_utils:is_feature_flag_enabled() of
    true ->  [{"/stream/connections", ?MODULE, []}];
    false -> []
  end.


web_ui() ->
  case rabbit_stream_management_utils:is_feature_flag_enabled() of
    true -> [{javascript, <<"stream.js">>}];
    false ->  rabbit_log:warning(
      "Unable to show the stream management plugin. The stream_queue feature flag is disabled. "++
      "You need to enable it then disable and re-enable the rabbitmq_stream_management plugin. ",
      "See https://www.rabbitmq.com/feature-flags.html ",
      []),
      []
  end.

%%--------------------------------------------------------------------

init(Req, _Opts) ->
    {cowboy_rest, rabbit_mgmt_cors:set_headers(Req, ?MODULE), #context{}}.

content_types_provided(ReqData, Context) ->
    {[{<<"application/json">>, to_json}], ReqData, Context}.

to_json(ReqData, Context) ->
    try
        Connections = do_connections_query(ReqData, Context),
        rabbit_mgmt_util:reply_list_or_paginate(Connections, ReqData, Context)
    catch
        {error, invalid_range_parameters, Reason} ->
            rabbit_mgmt_util:bad_request(iolist_to_binary(Reason), ReqData,
                                         Context)
    end.

is_authorized(ReqData, Context) ->
    rabbit_mgmt_util:is_authorized(ReqData, Context).

augmented(ReqData, Context) ->
    rabbit_mgmt_util:filter_conn_ch_list(
        rabbit_mgmt_db:get_all_connections(
            rabbit_mgmt_util:range_ceil(ReqData)),
        ReqData, Context).

do_connections_query(ReqData, Context) ->
    case rabbit_mgmt_util:disable_stats(ReqData) of
        false ->
            rabbit_stream_management_utils:keep_stream_connections(augmented(ReqData,
                                                                             Context));
        true ->
            TrackedStreamConnections =
                rabbit_stream_management_utils:keep_tracked_stream_connections(
                    rabbit_connection_tracking:list()),
            rabbit_mgmt_util:filter_tracked_conn_list(TrackedStreamConnections,
                                                      ReqData, Context)
    end.
