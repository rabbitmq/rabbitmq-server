%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_mqtt_collector).

-include("mqtt_machine.hrl").

-export([register/2, register/3, unregister/2, list/0, leave/1]).

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

unregister(ClientId, Pid) ->
    {ClusterName, _} = mqtt_node:server_id(),
    case ra_leaderboard:lookup_leader(ClusterName) of
        undefined ->
            ok;
        Leader ->
            send_ra_command(Leader, {unregister, ClientId, Pid}, no_correlation)
    end.

list() ->
    {ClusterName, _} = mqtt_node:server_id(),
     QF = fun (#machine_state{client_ids = Ids}) -> maps:to_list(Ids) end,
    case ra_leaderboard:lookup_leader(ClusterName) of
        undefined ->
            NodeIds = mqtt_node:all_node_ids(),
            case ra:leader_query(NodeIds, QF) of
                {ok, {_, Ids}, _} -> Ids;
                {timeout, _}      ->
                    _ = rabbit_log:debug("~s:list/0 leader query timed out",
                                     [?MODULE]),
                    []
            end;
        Leader ->
            case ra:leader_query(Leader, QF) of
                {ok, {_, Ids}, _} -> Ids;
                {error, _} ->
                    [];
                {timeout, _}      ->
                    _ = rabbit_log:debug("~s:list/0 leader query timed out",
                                     [?MODULE]),
                    []
            end
    end.

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
