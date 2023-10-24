%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2023 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_mgmt_wm_queues).

-export([init/2, to_json/2, content_types_provided/2, is_authorized/2,
         resource_exists/2, basic/1]).
-export([variances/2,
         augmented/2]).

-include_lib("rabbitmq_management_agent/include/rabbit_mgmt_records.hrl").
-include_lib("rabbit_common/include/rabbit.hrl").
-include_lib("rabbit/include/amqqueue.hrl").

-define(BASIC_COLUMNS, ["vhost", "name", "durable", "auto_delete", "exclusive",
                       "owner_pid", "arguments", "pid", "state"]).

-define(DEFAULT_SORT, ["vhost", "name"]).

%%--------------------------------------------------------------------

init(Req, State) ->
    Mode = case State of
               [] -> basic;
               [detailed] -> detailed
           end,
    {cowboy_rest, rabbit_mgmt_headers:set_common_permission_headers(Req, ?MODULE), {Mode, #context{}}}.

variances(Req, Context) ->
    {[<<"accept-encoding">>, <<"origin">>], Req, Context}.

content_types_provided(ReqData, Context) ->
   {rabbit_mgmt_util:responder_map(to_json), ReqData, Context}.

resource_exists(ReqData, {Mode, Context}) ->
    {case queues0(ReqData) of
         vhost_not_found -> false;
         _               -> true
     end, ReqData, {Mode, Context}}.

to_json(ReqData, {Mode, Context}) ->
    try
        Basic = basic_vhost_filtered(ReqData, Context),
        Data = rabbit_mgmt_util:augment_resources(Basic, ?DEFAULT_SORT,
                                                  ?BASIC_COLUMNS, ReqData,
                                                  Context, augment(Mode)),
        rabbit_mgmt_util:reply(Data, ReqData, {Mode, Context})
    catch
        {error, invalid_range_parameters, Reason} ->
            rabbit_mgmt_util:bad_request(iolist_to_binary(Reason), ReqData,
                                         {Mode, Context})
    end.

is_authorized(ReqData, {Mode, Context}) ->
    {Res, RD2, C2} = rabbit_mgmt_util:is_authorized_vhost(ReqData, Context),
    {Res, RD2, {Mode, C2}}.

%%--------------------------------------------------------------------
%% Exported functions

basic(ReqData) ->
    case rabbit_mgmt_util:disable_stats(ReqData) of
        false ->
            [rabbit_mgmt_format:queue(Q) || Q <- queues0(ReqData)] ++
                [rabbit_mgmt_format:queue(amqqueue:set_state(Q, down)) ||
                    Q <- down_queues(ReqData)];
        true ->
            case rabbit_mgmt_util:enable_queue_totals(ReqData) of
                false ->
                    [rabbit_mgmt_format:queue(Q) ++ policy(Q) || Q <- queues0(ReqData)] ++
                        [rabbit_mgmt_format:queue(amqqueue:set_state(Q, down)) ||
                            Q <- down_queues(ReqData)];
                true ->
                    [rabbit_mgmt_format:queue_info(Q) || Q <- queues_with_totals(ReqData)] ++
                        [rabbit_mgmt_format:queue(amqqueue:set_state(Q, down)) ||
                            Q <- down_queues(ReqData)]
            end
    end.

augmented(ReqData, Context) ->
    Fun = augment(basic),
    Fun(rabbit_mgmt_util:filter_vhost(basic(ReqData), ReqData, Context), ReqData).

%%--------------------------------------------------------------------
%% Private helpers

augment(Mode) ->
    fun(Basic, ReqData) ->
            case rabbit_mgmt_util:disable_stats(ReqData) of
                false ->
                    %% The reduced endpoint needs to sit behind a feature flag, as it calls a different data aggregation function that is used against all cluster nodes.
                    %% Data can be collected locally even if other nodes in the cluster do not, it's just a local ETS table. But it can't be queried until all nodes enable the FF.
                    IsEnabled = rabbit_feature_flags:is_enabled(detailed_queues_endpoint),
                    Stats = case {IsEnabled, Mode, rabbit_mgmt_util:columns(ReqData)} of
                                {false, _, _} -> detailed;
                                {_, detailed, _} -> detailed;
                                {_, _, all} -> basic;
                                _ -> detailed
                            end,
                    rabbit_log:warning("AUGMENT QUEUES is_enabled ~p stats ~p", [IsEnabled, Stats]),
                    rabbit_mgmt_db:augment_queues(Basic, rabbit_mgmt_util:range_ceil(ReqData),
                                                  Stats);
                true ->
                    Basic
            end
    end.

basic_vhost_filtered(ReqData, Context) ->
    rabbit_mgmt_util:filter_vhost(basic(ReqData), ReqData, Context).

queues0(ReqData) ->
    rabbit_mgmt_util:all_or_one_vhost(ReqData, fun rabbit_amqqueue:list/1).

queues_with_totals(ReqData) ->
    rabbit_mgmt_util:all_or_one_vhost(ReqData, fun collect_info_all/1).

collect_info_all(VHostPath) ->
    rabbit_amqqueue:collect_info_all(VHostPath, [name, durable, auto_delete, exclusive, owner_pid, arguments, type, state, policy, totals, type_specific]).

down_queues(ReqData) ->
    rabbit_mgmt_util:all_or_one_vhost(ReqData, fun rabbit_amqqueue:list_down/1).

policy(Q) ->
    case rabbit_policy:name(Q) of
        none -> [];
        Policy -> [{policy, Policy}]
    end.
