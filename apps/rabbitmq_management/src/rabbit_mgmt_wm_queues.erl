%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.
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

init(Req, _State) ->
    {cowboy_rest, rabbit_mgmt_headers:set_common_permission_headers(Req, ?MODULE), #context{}}.

variances(Req, Context) ->
    {[<<"accept-encoding">>, <<"origin">>], Req, Context}.

content_types_provided(ReqData, Context) ->
   {rabbit_mgmt_util:responder_map(to_json), ReqData, Context}.

resource_exists(ReqData, Context) ->
    {case queues0(ReqData) of
         vhost_not_found -> false;
         _               -> true
     end, ReqData, Context}.

to_json(ReqData, Context) ->
    try
        Basic = basic_vhost_filtered(ReqData, Context),
        Data = rabbit_mgmt_util:augment_resources(Basic, ?DEFAULT_SORT,
                                                  ?BASIC_COLUMNS, ReqData,
                                                  Context, fun augment/2),
        rabbit_mgmt_util:reply(Data, ReqData, Context)
    catch
        {error, invalid_range_parameters, Reason} ->
            rabbit_mgmt_util:bad_request(iolist_to_binary(Reason), ReqData,
                                         Context)
    end.

is_authorized(ReqData, Context) ->
    rabbit_mgmt_util:is_authorized_vhost(ReqData, Context).

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
    augment(rabbit_mgmt_util:filter_vhost(basic(ReqData), ReqData, Context), ReqData).

%%--------------------------------------------------------------------
%% Private helpers

augment(Basic, ReqData) ->
    case rabbit_mgmt_util:disable_stats(ReqData) of
        false ->
            rabbit_mgmt_db:augment_queues(Basic, rabbit_mgmt_util:range_ceil(ReqData),
                                          basic);
        true ->
            Basic
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
