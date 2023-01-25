%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2023 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_mqtt_collector).

-include("mqtt_machine.hrl").

-export([register/2, register/3, unregister/2,
         list/0, list_pids/0, leave/1]).

%%----------------------------------------------------------------------------
-spec register(term(), pid()) -> {ok, reference()} | {error, term()}.
register(ClientId, Pid) ->
    {ClusterName, _} = NodeId = mqtt_node:server_id(),
    case ra_leaderboard:lookup_leader(ClusterName) of
        undefined ->
            case ra:members(NodeId) of
                {ok, _, Leader} ->
                    register(Leader, ClientId, Pid);
                 _ = Error ->
                    Error
            end;
        Leader ->
            register(Leader, ClientId, Pid)
    end.

-spec register(ra:server_id(), term(), pid()) ->
    {ok, reference()} | {error, term()}.
register(ServerId, ClientId, Pid) ->
    Corr = make_ref(),
    send_ra_command(ServerId, {register, ClientId, Pid}, Corr),
    erlang:send_after(5000, self(), {ra_event, undefined, register_timeout}),
    {ok, Corr}.

-spec unregister(binary(), pid()) -> ok.
unregister(ClientId, Pid) ->
    {ClusterName, _} = mqtt_node:server_id(),
    case ra_leaderboard:lookup_leader(ClusterName) of
        undefined ->
            ok;
        Leader ->
            send_ra_command(Leader, {unregister, ClientId, Pid}, no_correlation)
    end.

-spec list_pids() -> [pid()].
list_pids() ->
    list(fun(#machine_state{pids = Pids}) -> maps:keys(Pids) end).

-spec list() -> term().
list() ->
    list(fun(#machine_state{client_ids = Ids}) -> maps:to_list(Ids) end).

list(QF) ->
    {ClusterName, _} = mqtt_node:server_id(),
    case ra_leaderboard:lookup_leader(ClusterName) of
        undefined ->
            NodeIds = mqtt_node:all_node_ids(),
            case ra:leader_query(NodeIds, QF) of
                {ok, {_, Result}, _} -> Result;
                {timeout, _}      ->
                    rabbit_log:debug("~ts:list/1 leader query timed out",
                                     [?MODULE]),
                    []
            end;
        Leader ->
            case ra:leader_query(Leader, QF) of
                {ok, {_, Result}, _} -> Result;
                {error, _} ->
                    [];
                {timeout, _}      ->
                    rabbit_log:debug("~ts:list/1 leader query timed out",
                                     [?MODULE]),
                    []
            end
    end.

-spec leave(binary()) ->  ok | timeout | nodedown.
leave(NodeBin) ->
    Node = binary_to_atom(NodeBin, utf8),
    ServerId = mqtt_node:server_id(),
    run_ra_command(ServerId, {leave, Node}),
    mqtt_node:leave(Node).

%%----------------------------------------------------------------------------
-spec run_ra_command(term(), term()) -> term() | {error, term()}.
run_ra_command(ServerId, RaCommand) ->
    case ra:process_command(ServerId, RaCommand) of
        {ok, Result, _} -> Result;
        _ = Error -> Error
    end.

-spec send_ra_command(term(), term(), term()) -> ok.
send_ra_command(ServerId, RaCommand, Correlation) ->
    ok = ra:pipeline_command(ServerId, RaCommand, Correlation, normal).
