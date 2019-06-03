%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License
%% at https://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and
%% limitations under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2007-2019 Pivotal Software, Inc.  All rights reserved.
%%
-module(mqtt_machine).
-behaviour(ra_machine).

-include_lib("ra/include/ra.hrl").

-export([init/1,
         apply/3,
         state_enter/2]).

-record(state, {client_ids = #{}}).

-type state() :: #state{}.

-type config() :: map().

-type reply() :: {ok, term()} | {error, term()}.
-type client_id() :: term().

-type command() :: {register, client_id(), pid()} |
                   {unregister, client_id(), pid()} |
                   list.

-spec init(config()) -> state().
init(_Conf) ->
    #state{}.

-spec apply(map(), command(), state()) ->
    {state(), reply(), ra_machine:effects()}.
apply(Meta, {register, ClientId, Pid}, #state{client_ids = Ids} = State0) ->
    {Effects, Ids1} =
        case maps:find(ClientId, Ids) of
            {ok, OldPid} when Pid =/= OldPid ->
                Effects0 = [{demonitor, process, OldPid},
                            {monitor, process, Pid},
                            {mod_call, gen_server2, cast, [OldPid, duplicate_id]}],
                {Effects0, maps:remove(ClientId, Ids)};
            error ->
              Effects0 = [{monitor, process, Pid}],
              {Effects0, Ids}
        end,
    State = State0#state{client_ids = maps:put(ClientId, Pid, Ids1)},
    {State, ok, Effects ++ snapshot_effects(Meta, State)};

apply(Meta, {unregister, ClientId, Pid}, #state{client_ids = Ids} = State0) ->
    State = case maps:find(ClientId, Ids) of
      {ok, Pid}         -> State0#state{client_ids = maps:remove(ClientId, Ids)};
      %% don't delete client id that might belong to a newer connection
      %% that kicked the one with Pid out
      {ok, _AnotherPid} -> State0;
      error             -> State0
    end,
    {State, ok, [{demonitor, process, Pid}] ++ snapshot_effects(Meta, State)};

apply(Meta, {down, DownPid, _}, #state{client_ids = Ids} = State0) ->
    Ids1 = maps:filter(fun (_ClientId, Pid) when Pid =:= DownPid ->
                               false;
                            (_, _) ->
                               true
                       end, Ids),
    State = State0#state{client_ids = Ids1},
    Delta = maps:keys(Ids) -- maps:keys(Ids1),
    Effects = lists:map(fun(Id) ->
                  [{mod_call, rabbit_log, debug,
                    ["MQTT connection with client id '~s' failed", [Id]]}] end, Delta),
    {State, ok, Effects ++ snapshot_effects(Meta, State)};

apply(Meta, {leave, Node}, #state{client_ids = Ids} = State0) ->
    Ids1 = maps:filter(fun (_ClientId, Pid) -> node(Pid) =/= Node end, Ids),
    Delta = maps:keys(Ids) -- maps:keys(Ids1),

    Effects = lists:foldl(fun (ClientId, Acc) ->
                          Pid = maps:get(ClientId, Ids),
                          [
                            {demonitor, process, Pid},
                            {mod_call, gen_server2, cast, [Pid, decommission_node]},
                            {mod_call, rabbit_log, debug,
                              ["MQTT will remove client ID '~s' from known "
                               "as its node has been decommissioned", [ClientId]]}
                          ]  ++ Acc
                          end, [], Delta),

    State = State0#state{client_ids = Ids1},
    {State, ok, Effects ++ snapshot_effects(Meta, State)};

apply(Meta, list, #state{client_ids = Ids} = State) ->
    {State, maps:to_list(Ids), snapshot_effects(Meta, State)};

apply(_Meta, Unknown, State) ->
    error_logger:error_msg("MQTT Raft state machine received unknown command ~p~n", [Unknown]),
    {State, {error, {unknown_command, Unknown}}, []}.

state_enter(leader, State) ->
    %% re-request monitors for all known pids, this would clean up
    %% records for all connections are no longer around, e.g. right after node restart
    [{monitor, process, Pid} || Pid <- all_pids(State)];
state_enter(_, _) ->
    [].

%% ==========================

-spec snapshot_effects(map(), state()) -> ra_machine:effects().
snapshot_effects(#{index := RaftIdx}, State) ->
    [{release_cursor, RaftIdx, State}].

all_pids(#state{client_ids = Ids}) ->
    maps:values(Ids).
