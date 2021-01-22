%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2020-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_stream_connections_vhost_mgmt).

-behaviour(rabbit_mgmt_extension).

-export([dispatcher/0,
         web_ui/0]).
-export([init/2,
         to_json/2,
         content_types_provided/2,
         resource_exists/2,
         is_authorized/2]).

-include_lib("rabbitmq_management_agent/include/rabbit_mgmt_records.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").

dispatcher() ->
    [{"/stream/connections/:vhost", ?MODULE, []}].

web_ui() ->
    [].

%%--------------------------------------------------------------------

init(Req, _Opts) ->
    {cowboy_rest, rabbit_mgmt_cors:set_headers(Req, ?MODULE), #context{}}.

content_types_provided(ReqData, Context) ->
    {[{<<"application/json">>, to_json}], ReqData, Context}.

resource_exists(ReqData, Context) ->
    {rabbit_vhost:exists(
         rabbit_mgmt_util:id(vhost, ReqData)),
     ReqData, Context}.

to_json(ReqData, Context) ->
    try
        rabbit_mgmt_util:reply_list(
            rabbit_stream_management_utils:keep_stream_connections(augmented(ReqData,
                                                                             Context)),
            ReqData, Context)
    catch
        {error, invalid_range_parameters, Reason} ->
            rabbit_mgmt_util:bad_request(iolist_to_binary(Reason), ReqData,
                                         Context)
    end.

is_authorized(ReqData, Context) ->
    rabbit_mgmt_util:is_authorized_vhost_visible_for_monitoring(ReqData,
                                                                Context).

augmented(ReqData, Context) ->
    rabbit_mgmt_util:filter_conn_ch_list(
        rabbit_mgmt_db:get_all_connections(
            rabbit_mgmt_util:range_ceil(ReqData)),
        ReqData, Context).
