%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%
-module(mqtt_machine).
-behaviour(ra_machine).

-include("mqtt_machine.hrl").

-export([version/0,
         which_module/1,
         init/1,
         apply/3,
         state_enter/2,
         notify_connection/2]).

-type state() :: #machine_state{}.

-type config() :: map().

-type reply() :: {ok, term()} | {error, term()}.
-type client_id() :: term().

-type command() :: {register, client_id(), pid()} |
                   {unregister, client_id(), pid()} |
                   list.
version() -> 1.

which_module(1) -> ?MODULE;
which_module(0) -> mqtt_machine_v0.

-spec init(config()) -> state().
init(_Conf) ->
    #machine_state{}.

-spec apply(map(), command(), state()) ->
    {state(), reply(), ra_machine:effects()}.
apply(_Meta, {register, ClientId, Pid},
      #machine_state{client_ids = Ids,
                     pids = Pids0} = State0) ->
    {Effects, Ids1, Pids} =
        case maps:find(ClientId, Ids) of
            {ok, OldPid} when Pid =/= OldPid ->
                Effects0 = [{demonitor, process, OldPid},
                            {monitor, process, Pid},
                            {mod_call, ?MODULE, notify_connection,
                             [OldPid, duplicate_id]}],
                Pids2 = case maps:take(OldPid, Pids0) of
                            error ->
                                Pids0;
                            {[ClientId], Pids1} ->
                                Pids1;
                            {ClientIds, Pids1} ->
                                Pids1#{ClientId => lists:delete(ClientId, ClientIds)}
                        end,
                Pids3 = maps:update_with(Pid, fun(CIds) -> [ClientId | CIds] end,
                                         [ClientId], Pids2),
                {Effects0, maps:remove(ClientId, Ids), Pids3};

            {ok, Pid}  ->
                {[], Ids, Pids0};
            error ->
                Pids1 = maps:update_with(Pid, fun(CIds) -> [ClientId | CIds] end,
                                         [ClientId], Pids0),
                Effects0 = [{monitor, process, Pid}],
                {Effects0, Ids, Pids1}
        end,
    State = State0#machine_state{client_ids = maps:put(ClientId, Pid, Ids1),
                                 pids = Pids},
    {State, ok, Effects};

apply(Meta, {unregister, ClientId, Pid}, #machine_state{client_ids = Ids,
                                                        pids = Pids0} = State0) ->
    State = case maps:find(ClientId, Ids) of
                {ok, Pid} ->
                    Pids = case maps:get(Pid, Pids0, undefined) of
                               undefined ->
                                   Pids0;
                               [ClientId] ->
                                   maps:remove(Pid, Pids0);
                               Cids ->
                                   Pids0#{Pid => lists:delete(ClientId, Cids)}
                           end,

                    State0#machine_state{client_ids = maps:remove(ClientId, Ids),
                                         pids = Pids};
                %% don't delete client id that might belong to a newer connection
                %% that kicked the one with Pid out
                {ok, _AnotherPid} ->
                    State0;
                error ->
                    State0
            end,
    Effects0 = [{demonitor, process, Pid}],
    %% snapshot only when the map has changed
    Effects = case State of
      State0 -> Effects0;
      _      -> Effects0 ++ snapshot_effects(Meta, State)
    end,
    {State, ok, Effects};

apply(_Meta, {down, DownPid, noconnection}, State) ->
    %% Monitor the node the pid is on (see {nodeup, Node} below)
    %% so that we can detect when the node is re-connected and discover the
    %% actual fate of the connection processes on it
    Effect = {monitor, node, node(DownPid)},
    {State, ok, Effect};

apply(Meta, {down, DownPid, _}, #machine_state{client_ids = Ids,
                                               pids = Pids0} = State0) ->
    case maps:get(DownPid, Pids0, undefined) of
        undefined ->
            {State0, ok, []};
        ClientIds ->
            Ids1 = maps:without(ClientIds, Ids),
            State = State0#machine_state{client_ids = Ids1,
                                         pids = maps:remove(DownPid, Pids0)},
            Effects = lists:map(fun(Id) ->
                                        [{mod_call, rabbit_log, debug,
                                          ["MQTT connection with client id '~s' failed", [Id]]}]
                                end, ClientIds),
            {State, ok, Effects ++ snapshot_effects(Meta, State)}
    end;

apply(_Meta, {nodeup, Node}, State) ->
    %% Work out if any pids that were disconnected are still
    %% alive.
    %% Re-request the monitor for the pids on the now-back node.
    Effects = [{monitor, process, Pid} || Pid <- all_pids(State), node(Pid) == Node],
    {State, ok, Effects};
apply(_Meta, {nodedown, _Node}, State) ->
    {State, ok};

apply(Meta, {leave, Node}, #machine_state{client_ids = Ids,
                                          pids = Pids0} = State0) ->
    {Keep, Remove} = maps:fold(
                       fun (ClientId, Pid, {In, Out}) ->
                               case node(Pid) =/= Node of
                                   true ->
                                       {In#{ClientId => Pid}, Out};
                                   false ->
                                       {In, Out#{ClientId => Pid}}
                               end
                       end, {#{}, #{}}, Ids),
    Effects = maps:fold(fun (ClientId, _Pid, Acc) ->
                                Pid = maps:get(ClientId, Ids),
                                [
                                 {demonitor, process, Pid},
                                 {mod_call, ?MODULE, notify_connection, [Pid, decommission_node]},
                                 {mod_call, rabbit_log, debug,
                                  ["MQTT will remove client ID '~s' from known "
                                   "as its node has been decommissioned", [ClientId]]}
                                ]  ++ Acc
                        end, [], Remove),

    State = State0#machine_state{client_ids = Keep,
                                 pids = maps:without(maps:keys(Remove), Pids0)},
    {State, ok, Effects ++ snapshot_effects(Meta, State)};
apply(_Meta, {machine_version, 0, 1}, {machine_state, Ids}) ->
    Pids = maps:fold(
             fun(Id, Pid, Acc) ->
                     maps:update_with(Pid,
                                      fun(CIds) -> [Id | CIds] end,
                                      [Id], Acc)
             end, #{}, Ids),
    {#machine_state{client_ids = Ids,
                    pids = Pids}, ok, []};
apply(_Meta, Unknown, State) ->
    logger:error("MQTT Raft state machine v1 received unknown command ~p", [Unknown]),
    {State, {error, {unknown_command, Unknown}}, []}.

state_enter(leader, State) ->
    %% re-request monitors for all known pids, this would clean up
    %% records for all connections are no longer around, e.g. right after node restart
    [{monitor, process, Pid} || Pid <- all_pids(State)];
state_enter(_, _) ->
    [].

%% ==========================

%% Avoids blocking the Raft leader.
notify_connection(Pid, Reason) ->
  spawn(fun() -> gen_server2:cast(Pid, Reason) end).

-spec snapshot_effects(map(), state()) -> ra_machine:effects().
snapshot_effects(#{index := RaftIdx}, State) ->
    [{release_cursor, RaftIdx, State}].

all_pids(#machine_state{client_ids = Ids}) ->
    maps:values(Ids).
