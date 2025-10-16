%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_mgmt_wm_health_check_below_node_connection_limit).

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
    ActiveConns = lists:foldl(
                    fun(Protocol, Acc) ->
                            Acc + protocol_connection_count(Protocol)
                    end, 0, [amqp, 'amqp/ssl']),
    Limit = rabbit_misc:get_env(rabbit, connection_max, infinity),
    case ActiveConns < Limit of
        true ->
            rabbit_mgmt_util:reply(#{status => ok}, ReqData, Context);
        false ->
            ?LOG_WARNING(
              "Node connection limit is reached. Active connections: ~w, "
              "limit: ~w",
              [ActiveConns, Limit]),
            Body = #{
                status => failed,
                reason => <<"node connection limit is reached">>
            },
            {Response, ReqData1, Context1} = rabbit_mgmt_util:reply(
                                               Body, ReqData, Context),
            {stop,
             cowboy_req:reply(
               ?HEALTH_CHECK_FAILURE_STATUS, #{}, Response, ReqData1),
             Context1}
    end.

protocol_connection_count(Protocol) ->
    Refs = rabbit_networking:ranch_refs_of_protocol(Protocol),
    lists:foldl(
      fun(Ref, Acc) ->
              #{active_connections := Count} = ranch:info(Ref),
              Acc + Count
      end, 0, Refs).
