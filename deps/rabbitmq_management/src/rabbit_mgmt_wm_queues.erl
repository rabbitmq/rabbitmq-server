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

-define(BASIC_COLUMNS,
        ["vhost",
         "name",
         "node",
         "durable",
         "auto_delete",
         "exclusive",
         "owner_pid",
         "arguments",
         "type",
         "pid",
         "state"]).

-define(DEFAULT_SORT, ["vhost", "name"]).

%%--------------------------------------------------------------------

init(Req, _State) ->
    {cowboy_rest, rabbit_mgmt_headers:set_common_permission_headers(Req, ?MODULE), #context{}}.

variances(Req, Context) ->
    {[<<"accept-encoding">>, <<"origin">>], Req, Context}.

content_types_provided(ReqData, Context) ->
   {rabbit_mgmt_util:responder_map(to_json), ReqData, Context}.

resource_exists(ReqData, Context) ->
    %% just checking that the vhost requested exists
    {case rabbit_mgmt_util:all_or_one_vhost(ReqData, fun (_) -> [] end) of
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
    %% rabbit_nodes:list_running/1 is a potentially slow function that performs
    %% a cluster wide query with a reasonably long (10s) timeout.
    %% TODO: replace with faster approximate function
    Running = rabbit_nodes:list_running(),
    Ctx = #{running_nodes => Running},
    FmtQ = fun (Q) -> rabbit_mgmt_format:queue(Q, Ctx) end,
    case rabbit_mgmt_util:disable_stats(ReqData) of
        false ->
            list_queues(ReqData, Running, FmtQ, FmtQ);
        true ->
            case rabbit_mgmt_util:enable_queue_totals(ReqData) of
                false ->
                    list_queues(ReqData, Running,
                                fun(Q) ->
                                        FmtQ(Q) ++
                                        %% TODO: just add policy name in
                                        %% rabbit_mgmt_format:queue/1?
                                        policy(Q)
                                end,
                                FmtQ);
                true ->
                    %% TODO: this is not optimised like the other code paths
                    %% most likely we can avoid the collector pattern by
                    %% simply querying the queue_metrics table for infos
                    [rabbit_mgmt_format:queue_info(Q)
                     || Q <- queues_with_totals(ReqData)] ++
                        [FmtQ(amqqueue:set_state(Q, down)) ||
                            Q <- down_queues(ReqData, Running)]
            end
    end.

list_queues(ReqData, Running, FormatRunningFun, FormatDownFun) ->
    [begin
         Pid = amqqueue:get_pid(Q),
         %% only queues whose leader pid is a on a non running node
         %% are considered "down", all other states should be passed
         %% as they are and the queue type impl will decide how to
         %% emit them.
         case not rabbit_amqqueue:is_local_to_node_set(Pid, Running) of
             false ->
                 FormatRunningFun(Q);
             true ->
                 FormatDownFun(amqqueue:set_state(Q, down))
         end
     end || Q <- all_queues(ReqData)].


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

all_queues(ReqData) ->
    rabbit_mgmt_util:all_or_one_vhost(ReqData, fun rabbit_amqqueue:list_all/1).

queues_with_totals(ReqData) ->
    rabbit_mgmt_util:all_or_one_vhost(ReqData, fun collect_info_all/1).

collect_info_all(VHostPath) ->
    rabbit_amqqueue:collect_info_all(VHostPath,
                                     [name, durable, auto_delete, exclusive,
                                      owner_pid, arguments, type, state,
                                      policy, totals, online, type_specific]).

down_queues(ReqData, Running) ->
    Fun = fun(VhostPath) -> rabbit_amqqueue:list_down(VhostPath, Running) end,
    rabbit_mgmt_util:all_or_one_vhost(ReqData, Fun).

policy(Q) ->
    case rabbit_policy:name(Q) of
        none -> [];
        Policy -> [{policy, Policy}]
    end.
