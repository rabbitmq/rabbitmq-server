%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2023 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_process).

-export([on_running_node/1,
         is_process_alive/1,
         is_process_hibernated/1,
         is_registered_process_alive/1]).

-spec on_running_node(Pid) -> OnRunningNode when
      Pid :: pid(),
      OnRunningNode :: boolean().
%% @doc Indicates if the specified process runs on a member of the cluster.
%%
%% @param Pid the PID of the process to check
%% @returns true if the process runs on one of the cluster members, false
%% otherwise.

on_running_node(Pid) ->
    Node = node(Pid),
    rabbit_nodes:is_running(Node).

%% This requires the process be in the same running cluster as us
%% (i.e. not partitioned or some random node).
%%
%% See also rabbit_misc:is_process_alive/1 which does not.

-spec is_process_alive(Pid) -> IsAlive when
      Pid :: pid() | {RegisteredName, Node},
      RegisteredName :: atom(),
      Node :: node(),
      IsAlive :: boolean().
%% @doc Indicates if the specified process is alive.
%%
%% Unlike {@link erlang:is_process_alive/1}, this function works with remote
%% processes and registered processes. However, the process must run on one of
%% the cluster members.
%%
%% @param Pid the PID or name of the process to check
%% @returns true if the process is alive runs on one of the cluster members,
%% false otherwise.

is_process_alive(Pid) when is_pid(Pid) ->
    on_running_node(Pid)
    andalso
    rpc:call(node(Pid), erlang, is_process_alive, [Pid]) =:= true;
is_process_alive({Name, Node}) when is_atom(Name) andalso is_atom(Node) ->
    case rabbit_nodes:is_running(Node) of
        true ->
            try
                erpc:call(Node, ?MODULE, is_registered_process_alive, [Name])
            catch
                error:{exception, undef, [{?MODULE, _, _, _} | _]} ->
                    rpc:call(
                      Node,
                      rabbit_mnesia, is_registered_process_alive, [Name])
            end;
        false ->
            false
    end.

-spec is_registered_process_alive(RegisteredName) -> IsAlive when
      RegisteredName :: atom(),
      IsAlive :: boolean().
%% @doc Indicates if the specified registered process is alive.
%%
%% The process must be local to this node.
%%
%% @param RegisteredName the name of the registered process
%% @returns true if the process is alive, false otherwise.

is_registered_process_alive(Name) ->
    is_pid(whereis(Name)).

-spec is_process_hibernated(Pid) -> IsHibernated when
      Pid :: pid(),
      IsHibernated :: boolean().
%% @doc Indicates if the specified process is hibernated.
%%
%% @param Pid the PID or name of the process to check
%% @returns true if the process is hibernated on one of the cluster members,
%% false otherwise.

is_process_hibernated(Pid) when is_pid(Pid) ->
    {current_function,{erlang,hibernate,3}} == erlang:process_info(Pid, current_function).
