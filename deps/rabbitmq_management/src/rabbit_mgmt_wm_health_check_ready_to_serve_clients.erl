%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

%% A composite health check that combines:
%% * GET /api/health/checks/is-in-service
%% * GET /api/health/checks/protocol-listener/amqp
%% * GET /api/health/checks/below-node-connection-limit

-module(rabbit_mgmt_wm_health_check_ready_to_serve_clients).

-export([init/2]).
-export([to_json/2, content_types_provided/2]).
-export([variances/2]).

-include_lib("kernel/include/logger.hrl").

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
    case check() of
        ok ->
            rabbit_mgmt_util:reply(#{status => ok}, ReqData, Context);
        {error, Body} ->
            {Response, ReqData1, Context1} = rabbit_mgmt_util:reply(
                                               Body, ReqData, Context),
            {stop,
             cowboy_req:reply(
               ?HEALTH_CHECK_FAILURE_STATUS, #{}, Response, ReqData1),
             Context1}
    end.

check() ->
    case rabbit:is_serving() of
        true ->
            RanchRefs0 = (
              rabbit_networking:ranch_refs_of_protocol(amqp) ++
              rabbit_networking:ranch_refs_of_protocol('amqp/ssl')),
            RanchRefs = [R || R <- RanchRefs0, R =/= undefined],
            case RanchRefs of
                [_ | _] ->
                    ActiveConns = lists:foldl(
                      fun(RanchRef, Acc) ->
                              #{active_connections := Count} = ranch:info(RanchRef),
                              Acc + Count
                      end, 0, RanchRefs),
                    Limit = rabbit_misc:get_env(rabbit, connection_max, infinity),
                    case ActiveConns < Limit of
                        true ->
                            ok;
                        false ->
                            ?LOG_WARNING(
                              "Node connection limit is reached. Active "
                              "connections: ~w, limit: ~w",
                              [ActiveConns, Limit]),
                            {error, #{status => failed,
                                      reason => <<"node connection limit is reached">>,
                                      connections => ActiveConns}}
                    end;
                [] ->
                    {error, #{status => failed,
                              reason => <<"no active listeners for AMQP/AMQPS">>}}
            end;
        false ->
            {error, #{status => failed,
                      reason => <<"the rabbit node is not currently available to serve">>}}
    end.
