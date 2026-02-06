%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_mgmt_wm_health_check_reached_target_cluster_size).

-export([init/2]).
-export([to_json/2, content_types_provided/2]).
-export([variances/2]).

-include("rabbit_mgmt.hrl").
-include_lib("rabbitmq_management_agent/include/rabbit_mgmt_records.hrl").

init(Req, _State) ->
    Req1 = rabbit_mgmt_headers:set_no_cache_headers(
             rabbit_mgmt_headers:set_common_permission_headers(
               Req, ?MODULE), ?MODULE),
    {cowboy_rest, Req1, #context{}}.

variances(Req, Context) ->
    {[<<"accept-encoding">>, <<"origin">>], Req, Context}.

content_types_provided(ReqData, Context) ->
   {rabbit_mgmt_util:responder_map(to_json), ReqData, Context}.

to_json(ReqData, Context) ->
    case rabbit_nodes:reached_target_cluster_size() of
        true ->
            rabbit_mgmt_util:reply(#{status => ok}, ReqData, Context);
        false ->
            Online = rabbit_nodes:running_count(),
            Target = rabbit_nodes:target_cluster_size_hint(),
            Msg = io_lib:format("target cluster size not reached: ~b of ~b nodes are online",
                                [Online, Target]),
            failure(Msg, Online, Target, ReqData, Context)
    end.

failure(Message, Online, Target, ReqData, Context) ->
    Body = #{
        status => failed,
        reason => rabbit_data_coercion:to_binary(Message),
        online => Online,
        target => Target
    },
    {Response, ReqData1, Context1} = rabbit_mgmt_util:reply(Body, ReqData, Context),
    {stop, cowboy_req:reply(?HEALTH_CHECK_FAILURE_STATUS, #{}, Response, ReqData1), Context1}.
