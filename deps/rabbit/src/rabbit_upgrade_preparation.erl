%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_upgrade_preparation).

-include_lib("kernel/include/logger.hrl").


-export([await_online_quorum_plus_one/1,
         with_minimum_quorum/0]).

%%
%% API
%%

-define(SAMPLING_INTERVAL, 200).
-define(LOGGING_FREQUENCY, ?SAMPLING_INTERVAL * 100).

await_online_quorum_plus_one(Timeout) ->
    Iterations = ceil(Timeout / ?SAMPLING_INTERVAL),
    do_await_safe_online_quorum(Iterations).

%%
%% Implementation
%%

online_members(Component) ->
    lists:filter(fun erlang:is_pid/1,
                 rabbit_misc:append_rpc_all_nodes(rabbit_nodes:list_running(),
                                                  erlang, whereis, [Component])).

endangered_critical_components() ->
    CriticalComponents = [rabbit_stream_coordinator] ++
                            case rabbit_feature_flags:is_enabled(khepri_db) of
                                true -> [rabbitmq_metadata];
                                false -> []
                            end,
    Nodes = rabbit_nodes:list_members(),
    lists:filter(fun (Component) ->
                         NumAlive = length(online_members(Component)),
                         ServerIds = lists:zip(lists:duplicate(length(Nodes), Component), Nodes),
                         case ra:members(ServerIds) of
                             {error, _E} ->
                                 %% we've asked all nodes about it; if we didn't get an answer,
                                 %% the component is probably not running at all
                                 %% (eg. rabbit_stream_coordinator is only started when the
                                 %% first straem is declared)
                                 false;
                             {ok, Members, _Leader} ->
                                 NumAlive =< (length(Members) div 2) + 1
                         end
                 end,
                 CriticalComponents).

do_await_safe_online_quorum(0) ->
    false;
do_await_safe_online_quorum(IterationsLeft) ->
    {EndangeredQueues, EndangeredCriticalComponents} = with_minimum_quorum(),
    case EndangeredQueues =:= [] andalso EndangeredCriticalComponents =:= [] of
        true -> true;
        false ->
            case IterationsLeft rem ?LOGGING_FREQUENCY of
                0 ->
                    case length(EndangeredQueues) of
                        0 -> ok;
                        N -> ?LOG_INFO("Waiting for ~p queues and streams to have quorum+1 replicas online. "
                                             "You can list them with `rabbitmq-diagnostics check_if_node_is_quorum_critical`", [N])
                    end,
                    case EndangeredCriticalComponents of
                        [] -> ok;
                        _ -> ?LOG_INFO("Waiting for the following critical components to have quorum+1 replicas online: ~p.",
                                             [endangered_critical_components()])
                    end;
                _ ->
                    ok
            end,
            timer:sleep(?SAMPLING_INTERVAL),
            do_await_safe_online_quorum(IterationsLeft - 1)
    end.

-spec with_minimum_quorum() -> {[amqqueue:amqqueue()], [atom()]}.
with_minimum_quorum() ->
    EndangeredQueues = rabbit_queue_type:list_with_minimum_quorum(),
    EndangeredCriticalComponents = endangered_critical_components(),
    {EndangeredQueues, EndangeredCriticalComponents}.
